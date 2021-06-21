/*  =========================================================================
    fty_ambient_location_server - Ambient location metrics server

    Copyright (C) 2014 - 2020 Eaton

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
    =========================================================================
*/

/// fty_ambient_location_server - Ambient location metrics server

#include "fty_ambient_location_server.h"
#include <cmath>
#include <ctime>
#include <fty_shm.h>
#include <mutex>
#include <fty_log.h>

std::mutex mtx_ambient_hashmap;

#define AMBIENT_LOCATION_TYPE_HUMIDITY 0
#define AMBIENT_LOCATION_TYPE_TEMP     1
#define AMBIENT_LOCATION_TYPE_BOTH     2


#define ANSI_COLOR_REDTHIN       "\x1b[0;31m"
#define ANSI_COLOR_WHITE_ON_BLUE "\x1b[44;97m"
#define ANSI_COLOR_BOLD          "\x1b[1;39m"
#define ANSI_COLOR_RED           "\x1b[1;31m"
#define ANSI_COLOR_GREEN         "\x1b[1;32m"
#define ANSI_COLOR_YELLOW        "\x1b[1;33m"
#define ANSI_COLOR_BLUE          "\x1b[1;34m"
#define ANSI_COLOR_MAGENTA       "\x1b[1;35m"
#define ANSI_COLOR_CYAN          "\x1b[1;36m"
#define ANSI_COLOR_LIGHTMAGENTA  "\x1b[1;95m"
#define ANSI_COLOR_RESET         "\x1b[0m"

//  Structure of our class

struct value
{
    double value;
    int    ttl;
};

struct ambient_values_t
{
    value in_temperature;
    value in_humidity;
    value out_temperature;
    value out_humidity;
};

//  --------------------------------------------------------------------------
//  Create a new fty_ambient_location_server

AmbientLocation::AmbientLocation():
    client(mlm_client_new())
{
}

/*
 * return values :
 * 1 - $TERM recieved
 * 0 - message processed and deleted
 */

static int s_ambloc_actor_commands(AmbientLocation* self, zmsg_t** message_p)
{
    assert(self);
    assert(message_p && *message_p);

    zmsg_t* message = *message_p;

    char* command = zmsg_popstr(message);
    if (!command) {
        zmsg_destroy(message_p);
        log_warning("Empty command.");
        return 0;
    }
    log_debug("Command : %s", command);
    if (streq(command, "$TERM")) {
        log_debug("Got $TERM");
        zmsg_destroy(message_p);
        zstr_free(&command);
        return 1;
    } else if (streq(command, "CONNECT")) {
        char* endpoint = zmsg_popstr(message);
        char* name     = zmsg_popstr(message);

        if (endpoint && name) {
            log_debug("ambient_actor: CONNECT: %s/%s", endpoint, name);
            int rv = mlm_client_connect(self->client, endpoint, 1000, name);

            if (rv == -1)
                log_error("mlm_client_connect failed\n");
        }

        zstr_free(&endpoint);
        zstr_free(&name);
    } else if (streq(command, "CONSUMER")) {
        char* stream = zmsg_popstr(message);
        char* regex  = zmsg_popstr(message);

        if (stream && regex) {
            log_debug("CONSUMER: %s/%s", stream, regex);
            int rv = mlm_client_set_consumer(self->client, stream, regex);
            if (rv == -1)
                log_error("mlm_set_consumer failed");
        }

        zstr_free(&stream);
        zstr_free(&regex);
    } else if (streq(command, "START")) {
        zmsg_t* msg = zmsg_new();
        zmsg_addstr(msg, "$all");
        int rv = mlm_client_sendto(self->client, "asset-agent", "REPUBLISH", nullptr, 5000, &msg);
        if (rv != 0) {
            log_error("Request assets list failed");
            return 1;
        } else
            log_debug("Assets list request sent successfully");
        self->ambient_calculation = zactor_new(ambient_location_calculation, self);
    } else {
        log_error("Unknown actor command: %s.\n", command);
    }

    zstr_free(&command);
    zmsg_destroy(message_p);
    return 0;
}

static void s_remove_from_cache(AmbientLocation* self, std::string name, int type)
{
    if (type == AMBIENT_LOCATION_TYPE_HUMIDITY || type == AMBIENT_LOCATION_TYPE_BOTH) {
        if (self->cache.at(name).second.first != nullptr) {
            fty_proto_destroy(&self->cache.at(name).second.first);
            self->cache.at(name).second.first = nullptr;
        }
    }
    if (type == AMBIENT_LOCATION_TYPE_TEMP || type == AMBIENT_LOCATION_TYPE_BOTH) {
        if (self->cache.at(name).second.second != nullptr) {
            fty_proto_destroy(&self->cache.at(name).second.second);
            self->cache.at(name).second.second = nullptr;
        }
    }
}

static void s_publish_value(std::string type, std::string unit, std::string name, double value, int ttl)
{
    fty_proto_t* n_met = fty_proto_new(FTY_PROTO_METRIC);
    fty_proto_set_name(n_met, name.c_str());
    fty_proto_set_type(n_met, type.c_str());
    fty_proto_set_value(n_met, "%.2f", value);
    fty_proto_set_unit(n_met, "%s", unit.c_str());
    fty_proto_set_ttl(n_met, uint32_t(ttl));
    fty_proto_set_time(n_met, uint64_t(std::time(nullptr)));

    char* aux_log = nullptr;
    asprintf(&aux_log, "%s@%s (value: %s%s, ttl: %u)", fty_proto_type(n_met), fty_proto_name(n_met),
        fty_proto_value(n_met), fty_proto_unit(n_met), fty_proto_ttl(n_met));

    int rv = fty::shm::write_metric(n_met);
    if (rv != 0) {
        log_error(ANSI_COLOR_RED "SHM publish failed (%s)" ANSI_COLOR_RESET, aux_log);
    } else {
        log_debug(ANSI_COLOR_YELLOW "SHM publish %s" ANSI_COLOR_RESET, aux_log);
    }
    zstr_free(&aux_log);
    fty_proto_destroy(&n_met);
}

// return false if name is not a sensor
static bool s_get_cache_value(AmbientLocation* self, std::string name, int typeMetric, ambient_values_t& result)
{
    auto sensor = self->cache.find(name);
    if (sensor == self->cache.end()) {
        return false;
    }

    // it's a sensor
    fty_proto_t* sensor_value = nullptr;
    if (typeMetric == AMBIENT_LOCATION_TYPE_HUMIDITY) {
        sensor_value = sensor->second.second.first;
    } else {
        sensor_value = sensor->second.second.second;
    }

    if (sensor_value == nullptr) {
        // no metric in cache
        return true;
    }
    time_t valid_till = time_t(fty_proto_time(sensor_value) + fty_proto_ttl(sensor_value));
    if (time(nullptr) > valid_till) {
        // the metric is too old
        s_remove_from_cache(self, name, typeMetric);
        return true;
    }

    // we have a valid metric, get the data
    const char* value = fty_proto_value(sensor_value);
    char*       end;
    errno         = 0;
    double dvalue = strtod(value, &end);

    if (errno == ERANGE || end == value || *end != '\0') {
        log_info("cannot convert value '%s' to double, ignore message\n", value);
        fty_proto_print(sensor_value);
        return true;
    }

    std::string function = sensor->second.first;
    // std::string type = fty_proto_type(sensor_value);

    if (typeMetric == AMBIENT_LOCATION_TYPE_HUMIDITY) {
        if (function == "input") {
            result.in_humidity.value = dvalue;
            result.in_humidity.ttl   = int(fty_proto_ttl(sensor_value));
        } else if (function == "output") {
            result.out_humidity.value = dvalue;
            result.out_humidity.ttl   = int(fty_proto_ttl(sensor_value));
        }
    } else {
        if (function == "input") {
            result.in_temperature.value = dvalue;
            result.in_temperature.ttl   = int(fty_proto_ttl(sensor_value));
        } else if (function == "output") {
            result.out_temperature.value = dvalue;
            result.out_temperature.ttl   = int(fty_proto_ttl(sensor_value));
        }
    }
    return true;
}

static ambient_values_t s_compute_values(AmbientLocation* self, std::string name)
{
    ambient_values_t result;
    result.in_humidity.value     = std::nan("");
    result.out_humidity.value    = std::nan("");
    result.in_temperature.value  = std::nan("");
    result.out_temperature.value = std::nan("");
    result.in_humidity.ttl       = 0;
    result.out_humidity.ttl      = 0;
    result.in_temperature.ttl    = 0;
    result.out_temperature.ttl   = 0;
    // if name is a sensor, both humidity and temperature will see it as it is even if we don't have data in both
    if (s_get_cache_value(self, name, AMBIENT_LOCATION_TYPE_HUMIDITY, result)) {
        s_get_cache_value(self, name, AMBIENT_LOCATION_TYPE_TEMP, result);
        return result;
    }

    // not a sensor, must be a location
    if (self->m_list_contents.count(name) == 0) {
        // should not happend
        return result;
    }
    std::vector<std::string> content_list = self->m_list_contents.at(name);
    int                      outtemp_n    = 0;
    int                      outhum_n     = 0;
    int                      intemp_n     = 0;
    int                      inhum_n      = 0;
    result.in_temperature.value           = 0;
    result.out_temperature.value          = 0;
    result.in_humidity.value              = 0;
    result.out_humidity.value             = 0;
    for (auto& content : content_list) {
        ambient_values_t result_temp = s_compute_values(self, content);
        if (!std::isnan(result_temp.out_temperature.value)) {
            outtemp_n++;
            result.out_temperature.value += result_temp.out_temperature.value;
            result.out_temperature.ttl = result_temp.out_temperature.ttl;
        }
        if (!std::isnan(result_temp.out_humidity.value)) {
            outhum_n++;
            result.out_humidity.value += result_temp.out_humidity.value;
            result.out_humidity.ttl = result_temp.out_humidity.ttl;
        }
        if (!std::isnan(result_temp.in_temperature.value)) {
            intemp_n++;
            result.in_temperature.value += result_temp.in_temperature.value;
            result.in_temperature.ttl = result_temp.in_temperature.ttl;
        }
        if (!std::isnan(result_temp.in_humidity.value)) {
            inhum_n++;
            result.in_humidity.value += result_temp.in_humidity.value;
            result.in_humidity.ttl = result_temp.in_humidity.ttl;
        }
    }

    if (outtemp_n == 0) {
        result.out_temperature.value = std::nan("");
    } else {
        result.out_temperature.value = result.out_temperature.value / outtemp_n;
        if (name.find("rack") != std::string::npos || name.find("row") != std::string::npos)
            s_publish_value(
                "average.temperature-output", "C", name, result.out_temperature.value, result.out_temperature.ttl);
    }

    if (outhum_n == 0) {
        result.out_humidity.value = std::nan("");
    } else {
        result.out_humidity.value = result.out_humidity.value / outhum_n;
        if (name.find("rack") != std::string::npos || name.find("row") != std::string::npos)
            s_publish_value("average.humidity-output", "%", name, result.out_humidity.value, result.out_humidity.ttl);
    }

    if (intemp_n == 0) {
        result.in_temperature.value = std::nan("");
    } else {
        result.in_temperature.value = result.in_temperature.value / intemp_n;
        if (name.find("rack") != std::string::npos || name.find("row") != std::string::npos)
            s_publish_value(
                "average.temperature-input", "C", name, result.in_temperature.value, result.in_temperature.ttl);
    }

    if (inhum_n == 0) {
        result.in_humidity.value = std::nan("");
    } else {
        result.in_humidity.value = result.in_humidity.value / inhum_n;
        if (name.find("rack") != std::string::npos || name.find("row") != std::string::npos)
            s_publish_value("average.humidity-input", "%", name, result.in_humidity.value, result.in_humidity.ttl);
    }

    if (name.find("rack") == std::string::npos) {
        double humidity      = 0;
        double temperature   = 0;
        int    n_humidity    = 0;
        int    n_temperature = 0;
        if (!std::isnan(result.out_humidity.value)) {
            n_humidity++;
            humidity = result.out_humidity.value;
        }
        if (!std::isnan(result.in_humidity.value)) {
            n_humidity++;
            humidity += result.in_humidity.value;
            if (result.out_humidity.ttl == 0)
                result.out_humidity.ttl = result.in_humidity.ttl;
        }
        if (!std::isnan(result.out_temperature.value)) {
            n_temperature++;
            temperature = result.out_temperature.value;
        }
        if (!std::isnan(result.in_temperature.value)) {
            n_temperature++;
            temperature += result.in_temperature.value;
            if (result.out_temperature.ttl == 0)
                result.out_temperature.ttl = result.in_temperature.ttl;
        }

        if (humidity == 0)
            result.out_humidity.value = std::nan("");
        else {
            result.out_humidity.value = humidity / n_humidity;
            s_publish_value("average.humidity", "%", name, result.out_humidity.value, result.out_humidity.ttl);
        }

        if (temperature == 0)
            result.out_temperature.value = std::nan("");
        else {
            result.out_temperature.value = temperature / n_temperature;
            s_publish_value("average.temperature", "C", name, result.out_temperature.value, result.out_temperature.ttl);
        }
    }

    return result;
}

static int s_remove_asset(AmbientLocation* self, fty_proto_t* bmsg)
{
    log_debug("REMOVE ASSET");
    if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_TYPE, ""), "datacenter")) {
        for (unsigned int i = 0; i < self->datacenters.size(); i++) {
            if (self->datacenters[i] == fty_proto_name(bmsg)) {
                self->datacenters.erase(self->datacenters.begin() + i);
                return 0;
            }
        }
        return -1;
    }
    auto val = self->containers.find(fty_proto_name(bmsg));
    if (val == self->containers.end()) {
        // We don't know this asset
        return -1;
    }
    auto got_list = self->m_list_contents.find(val->second);
    // for safety reason, should always happened.
    if (got_list != self->m_list_contents.end()) {
        for (unsigned int i = 0; i < got_list->second.size(); i++) {
            if (got_list->second[i] == fty_proto_name(bmsg)) {
                got_list->second.erase(got_list->second.begin() + i);
                return 0;
            }
        }
    }
    return -1;
}

static int s_create_asset(AmbientLocation* self, fty_proto_t* bmsg)
{
    log_debug("CREATE ASSET");
    if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_TYPE, ""), "datacenter")) {
        self->datacenters.push_back(fty_proto_name(bmsg));
        return 0;
    }
    std::string parent;
    if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_SUBTYPE, ""), "sensor")) {
        parent = fty_proto_ext_string(bmsg, "logical_asset", "");
    } else {
        parent = fty_proto_aux_string(bmsg, "parent_name.1", "");
    }
    // should never happened
    if (parent == "")
        return -1;

    self->containers[fty_proto_name(bmsg)] = parent;

    auto got_list = self->m_list_contents.find(parent);
    if (got_list == self->m_list_contents.end()) {
        std::vector<std::string> new_list(1, fty_proto_name(bmsg));
        self->m_list_contents[parent] = new_list;
    } else {
        got_list->second.push_back(fty_proto_name(bmsg));
    }

    return 0;
}


static void s_ambloc_actor_stream(AmbientLocation* self, zmsg_t** message_p)
{
    // log_debug("s_ambloc_actor_stream");

    fty_proto_t* bmsg = fty_proto_decode(message_p);
    if (!bmsg) {
        log_error("Get a stream message that is not fty_proto typed");
        return;
    }

    if (streq(mlm_client_address(self->client), FTY_PROTO_STREAM_METRICS_SENSOR)) {

        std::string sensor_name = fty_proto_aux_string(bmsg, "sname", "");
        std::string type        = fty_proto_type(bmsg);

        log_debug("METRIC SENSOR message (asset: %s, type: %s)", sensor_name.c_str(), type.c_str());

        bool metric_in_cache = false;

        mtx_ambient_hashmap.lock();
        if (self->cache.count(sensor_name) != 0) {
            if (type.find("humidity") != std::string::npos) {
                s_remove_from_cache(self, sensor_name, AMBIENT_LOCATION_TYPE_HUMIDITY);
                self->cache.at(sensor_name).second.first = fty_proto_dup(bmsg);
                metric_in_cache                          = true;
            } else if (type.find("temperature") != std::string::npos) {
                s_remove_from_cache(self, sensor_name, AMBIENT_LOCATION_TYPE_TEMP);
                self->cache.at(sensor_name).second.second = fty_proto_dup(bmsg);
                metric_in_cache                           = true;
            }
        }
        mtx_ambient_hashmap.unlock();

        // PQSWMBT-3723: if sensor metric is handled, publish it in shared memory.
        // metric (or quantity) ex.: 'humidity.default@sensor-241', 'temperature.default@sensor-372'
        if (metric_in_cache) {
            const char* value_s = fty_proto_value(bmsg);
            double      value;
            int         r = sscanf((value_s ? value_s : ""), "%lf", &value);
            if (r == 1) {
                // here, sensor metric type is like 'temperature.N' or 'humidity.N'
                // where N is the index (offset 0) related to its device owner (edpu, ups).
                // we normalize the metric quantity to 'default'.
                const char* quantity = nullptr;
                if (type.find("temperature") != std::string::npos)
                    quantity = "temperature.default";
                else if (type.find("humidity") != std::string::npos)
                    quantity = "humidity.default";
                if (quantity)
                    s_publish_value(quantity, fty_proto_unit(bmsg), sensor_name, value, int(fty_proto_ttl(bmsg)));
            }
        }
        // end PQSWMBT-3723
    } else if (fty_proto_id(bmsg) == FTY_PROTO_ASSET) {

        log_debug("PROTO ASSET message");

        if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_TYPE, ""), "device") &&
            !streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_SUBTYPE, ""), "sensor")) {
            // we are only interested by containers and sensor.
            log_debug("PROTO ASSET message ignored (asset: '%s', type: '%s')", fty_proto_name(bmsg),
                fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_TYPE, ""));

            fty_proto_destroy(&bmsg);
            return;
        }

        mtx_ambient_hashmap.lock();
        if (streq(fty_proto_operation(bmsg), FTY_PROTO_ASSET_OP_DELETE) ||
            streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_STATUS, "active"), "inactive") ||
            streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_STATUS, "active"), "retired")) {
            s_remove_asset(self, bmsg);
        } else if (streq(fty_proto_operation(bmsg), FTY_PROTO_ASSET_OP_CREATE) ||
                   streq(fty_proto_operation(bmsg), FTY_PROTO_ASSET_OP_UPDATE)) {
            s_remove_asset(self, bmsg);
            int ret = s_create_asset(self, bmsg);
            if (ret != -1 && streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_SUBTYPE, ""), "sensor")) {
                auto sensor = self->cache.find(fty_proto_name(bmsg));
                if (sensor != self->cache.end()) {
                    sensor->second.first = fty_proto_ext_string(bmsg, "sensor_function", "");
                } else {
                    self->cache[fty_proto_name(bmsg)];
                    self->cache.at(fty_proto_name(bmsg)).first = fty_proto_ext_string(bmsg, "sensor_function", "");
                    self->cache.at(fty_proto_name(bmsg)).second.first  = nullptr;
                    self->cache.at(fty_proto_name(bmsg)).second.second = nullptr;
                }
            }
        }
        mtx_ambient_hashmap.unlock();
    } else {
        log_debug("Get a stream message from %s (unhandled)", mlm_client_address(self->client));
    }
    fty_proto_destroy(&bmsg);
}

void ambient_location_calculation(zsock_t* pipe, void* args)
{
    AmbientLocation* self = reinterpret_cast<AmbientLocation*>(args);
    assert(self);
    zpoller_t* poller = zpoller_new(pipe, nullptr);
    assert(poller);
    zsock_signal(pipe, 0);
    log_info("calculation_actor: Started");
    while (!zsys_interrupted) {
        self->timeout_ms = fty_get_polling_interval() * 1000;
        void* which      = zpoller_wait(poller, self->timeout_ms);
        if (which == nullptr) {
            if (zpoller_terminated(poller) || zsys_interrupted) {
                log_info("calculation_actor: Terminating.");
                break;
            } else {

                log_info("Starting calculation");
                // timeout, so we must calculate
                // we want to be consistant for each datacenters
                mtx_ambient_hashmap.lock();
                for (auto& datacenter : self->datacenters) {
                    s_compute_values(self, datacenter);
                }
                mtx_ambient_hashmap.unlock();
                log_info("End of calculation");
            }
        } else if (which == pipe) {
            zmsg_t* msg = zmsg_recv(pipe);
            if (!msg)
                break;

            char* command = zmsg_popstr(msg);
            if (!command) {
                zmsg_destroy(&msg);
                log_warning("Empty command in calculation.");
            }
            log_debug("Command : %s", command);
            if (streq(command, "$TERM")) {
                log_debug("Got $TERM");
                zmsg_destroy(&msg);
                zstr_free(&command);
                break;
            } else {
                log_debug("calculation actor : Unknow command");
                zmsg_destroy(&msg);
                zstr_free(&command);
            }
        }
    }
    zpoller_destroy(&poller);
    log_info("calculation_actor: Ended");
}


// --------------------------------------------------------------------------
// Create a new fty_ambient_location_server
void fty_ambient_location_server(zsock_t* pipe, void* /*args*/)
{
    AmbientLocation* self = new AmbientLocation();
    // AmbientLocation *self = fty_ambient_location_server_new ();
    assert(self);

    zpoller_t* poller = zpoller_new(pipe, mlm_client_msgpipe(self->client), nullptr);
    assert(poller);

    zsock_signal(pipe, 0);
    log_info("ambient_actor: Started");
    //    poller timeout
    while (!zsys_interrupted) {
        self->timeout_ms = fty_get_polling_interval() * 1000;
        void* which      = zpoller_wait(poller, self->timeout_ms);
        if (which == nullptr) {
            if (zpoller_terminated(poller) || zsys_interrupted) {
                log_info("ambient_actor: Terminating.");
                break;
            }
        } else if (which == pipe) {
            log_trace("which == pipe");
            zmsg_t* msg = zmsg_recv(pipe);
            if (!msg)
                break;

            int rv = s_ambloc_actor_commands(self, &msg);
            if (rv == 1)
                break;
            continue;
        } else if (which == mlm_client_msgpipe(self->client)) {

            zmsg_t* msg = mlm_client_recv(self->client);
            if (!msg)
                break;

            if (!fty_proto_is(msg)) {
                zmsg_destroy(&msg);
                continue;
            } else {
                s_ambloc_actor_stream(self, &msg);
            }
        }
    }
    zpoller_destroy(&poller);
    delete self;
    // fty_ambient_location_server_destroy(&self);
    log_info("ambient_actor: Ended");
}


//  --------------------------------------------------------------------------
//  Destroy the fty_ambient_location_server
AmbientLocation::~AmbientLocation()
{
    zactor_destroy(&this->ambient_calculation);
    mlm_client_destroy(&this->client);
    for (auto& sensor : this->cache) {
        s_remove_from_cache(this, sensor.first, AMBIENT_LOCATION_TYPE_BOTH);
    }
    log_info("ambient destroyed");
}

