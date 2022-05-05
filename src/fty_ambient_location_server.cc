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
#include <fty_proto.h>
#include <fty_shm.h>
#include <fty_log.h>
#include <czmq.h>
#include <malamute.h>
#include <cmath>
#include <ctime>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

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

#define AMBIENT_LOCATION_TYPE_HUMIDITY 0
#define AMBIENT_LOCATION_TYPE_TEMP     1
#define AMBIENT_LOCATION_TYPE_BOTH     2

static std::mutex mtx_ambient_hashmap;

//fwd decl.
class AmbientLocation;
static void s_remove_from_cache(AmbientLocation* self, std::string name, int type);
static void s_ambient_location_calculation(zsock_t* pipe, void* args);

// our class

struct value_t {
    double value{std::nan("")};
    int    ttl{0};
};

struct ambient_values_t {
    value_t in_temperature;
    value_t in_humidity;
    value_t out_temperature;
    value_t out_humidity;
};

class AmbientLocation
{
public:
    AmbientLocation(): client(mlm_client_new()), ambient_calculation(nullptr) {}

    ~AmbientLocation() {
        zactor_destroy(&ambient_calculation);
        mlm_client_destroy(&client);
        for (auto& sensor : cache) {
            s_remove_from_cache(this, sensor.first, AMBIENT_LOCATION_TYPE_BOTH);
        }
    }

    using Containers     = std::unordered_map<std::string, std::string>;
    using ContainersList = std::unordered_map<std::string, std::vector<std::string>>;
    using Cache       = std::unordered_map<std::string, std::pair<std::string, std::pair<fty_proto_t*, fty_proto_t*>>>;
    using Datacenters = std::vector<std::string>;

    mlm_client_t*  client{nullptr};
    zactor_t*      ambient_calculation{nullptr};

    Containers     containers;
    ContainersList m_list_contents;
    Cache          cache;
    Datacenters    datacenters;
};

// handle actor commands
// return:
// 1 - $TERM recieved
// 0 - message processed and deleted
static int s_handle_actor_commands(AmbientLocation* self, zmsg_t** message_p)
{
    assert(self);
    assert(message_p && (*message_p));

    zmsg_t* message = *message_p;

    char* command = zmsg_popstr(message);
    log_trace("Command: %s", command);

    int ret = 0;

    if (!command) {
        log_warning("command is NULL");
    }
    else if (streq(command, "$TERM")) {
        //log_trace("Got $TERM");
        ret = 1;
    }
    else if (streq(command, "CONNECT")) {
        char* endpoint = zmsg_popstr(message);
        char* name     = zmsg_popstr(message);

        if (endpoint && name) {
            log_debug("%s: %s %s", command, endpoint, name);
            int rv = mlm_client_connect(self->client, endpoint, 1000, name);

            if (rv == -1) {
                log_error("mlm_client_connect failed");
            }
        }

        zstr_free(&endpoint);
        zstr_free(&name);
    }
    else if (streq(command, "CONSUMER")) {
        char* stream = zmsg_popstr(message);
        char* regex  = zmsg_popstr(message);

        if (stream && regex) {
            log_debug("%s: %s %s", command, stream, regex);
            int rv = mlm_client_set_consumer(self->client, stream, regex);
            if (rv == -1) {
                log_error("mlm_set_consumer failed");
            }
        }

        zstr_free(&stream);
        zstr_free(&regex);
    }
    else if (streq(command, "START")) {
        log_debug("%s", command);
        zmsg_t* msg = zmsg_new();
        zmsg_addstr(msg, "$all");
        int rv = mlm_client_sendto(self->client, "asset-agent", "REPUBLISH", nullptr, 5000, &msg);
        zmsg_destroy(&msg);
        if (rv != 0) {
            log_error("Request assets REPUBLISH failed");
            ret = 1; // term
        }
        else {
            log_debug("Request assets REPUBLISH sent successfully");
            zactor_destroy(&self->ambient_calculation);
            self->ambient_calculation = zactor_new(s_ambient_location_calculation, self);
        }
    }
    else {
        log_error("Unknown command: %s.", command);
    }

    zstr_free(&command);
    zmsg_destroy(message_p);

    return ret; // 0: ok, 1: $TERM
}

static void s_remove_from_cache(AmbientLocation* self, std::string name, int type)
{
    assert(self);
    log_debug("remove from cache (%s, type: %d)", name.c_str(), type);

    auto it = self->cache.find(name);
    if (it == self->cache.end()) {
        log_debug("%s not found in cache", name.c_str());
        return;
    }

    if (type == AMBIENT_LOCATION_TYPE_HUMIDITY || type == AMBIENT_LOCATION_TYPE_BOTH) {
        fty_proto_destroy(&(it->second.second.first));
    }
    if (type == AMBIENT_LOCATION_TYPE_TEMP || type == AMBIENT_LOCATION_TYPE_BOTH) {
        fty_proto_destroy(&(it->second.second.second));
    }
}

static void s_publish_value(const std::string& type, const std::string& unit, const std::string& name, double value, int ttl)
{
    fty_proto_t* n_met = fty_proto_new(FTY_PROTO_METRIC);
    if (!n_met) {
        log_error("SHM publish: new METRIC failed (%s)", name.c_str());
        return;
    }

    fty_proto_set_name(n_met, name.c_str());
    fty_proto_set_type(n_met, type.c_str());
    fty_proto_set_value(n_met, "%.2f", value);
    fty_proto_set_unit(n_met, "%s", unit.c_str());
    fty_proto_set_ttl(n_met, uint32_t(ttl));
    fty_proto_set_time(n_met, uint64_t(std::time(nullptr)));

    char* aux_log = NULL;
    asprintf(&aux_log, "%s@%s (value: %s%s, ttl: %u)",
        fty_proto_type(n_met), fty_proto_name(n_met),
        fty_proto_value(n_met), fty_proto_unit(n_met),
        fty_proto_ttl(n_met));

    int rv = fty::shm::write_metric(n_met);
    if (rv != 0) {
        log_error(ANSI_COLOR_RED "SHM publish failed (%s)" ANSI_COLOR_RESET, aux_log);
    } else {
        log_debug(ANSI_COLOR_YELLOW "SHM publish %s" ANSI_COLOR_RESET, aux_log);
    }

    zstr_free(&aux_log);
    fty_proto_destroy(&n_met);
}

// return false if name is not in cache
static bool s_get_cache_value(AmbientLocation* self, std::string name, int typeMetric, ambient_values_t& result)
{
    assert(self);
    auto sensor = self->cache.find(name);
    if (sensor == self->cache.end()) {
        return false;
    }

    // it's a sensor
    fty_proto_t* sensor_value = nullptr;
    if (typeMetric == AMBIENT_LOCATION_TYPE_HUMIDITY) {
        sensor_value = sensor->second.second.first;
    } else { // assume temperature
        sensor_value = sensor->second.second.second;
    }
    if (!sensor_value) { // no metric in cache
        return true;
    }

    time_t valid_till = time_t(fty_proto_time(sensor_value) + fty_proto_ttl(sensor_value));
    if (time(nullptr) > valid_till) {
        // the metric is too old
        s_remove_from_cache(self, name, typeMetric);
        return true;
    }

    // we have a valid metric, get the value
    double dvalue = 0;
    {
        const char* value = fty_proto_value(sensor_value);
        char* end = NULL;
        errno = 0;
        dvalue = strtod(value, &end);

        if (errno == ERANGE || end == value || *end != '\0') {
            log_info("cannot convert value '%s' to double, ignore message", value);
            fty_proto_print(sensor_value);
            return true;
        }
    }

    std::string sensor_function = sensor->second.first;
    log_trace("%s: sensor_function='%s'", name.c_str(), sensor_function.c_str());

    if (typeMetric == AMBIENT_LOCATION_TYPE_HUMIDITY) {
        if (sensor_function == "input") {
            result.in_humidity.value = dvalue;
            result.in_humidity.ttl   = int(fty_proto_ttl(sensor_value));
        } else if (sensor_function == "output") {
            result.out_humidity.value = dvalue;
            result.out_humidity.ttl   = int(fty_proto_ttl(sensor_value));
        }
    }
    else { // assume temperature
        if (sensor_function == "input") {
            result.in_temperature.value = dvalue;
            result.in_temperature.ttl   = int(fty_proto_ttl(sensor_value));
        } else if (sensor_function == "output") {
            result.out_temperature.value = dvalue;
            result.out_temperature.ttl   = int(fty_proto_ttl(sensor_value));
        }
    }

    return true;
}

static ambient_values_t s_compute_values(AmbientLocation* self, std::string name)
{
    assert(self);
    log_debug("compute values (%s)", name.c_str());

    ambient_values_t result; // values default is (nan, 0)

    // if name is a sensor, both humidity and temperature will see it
    // as it is even if we don't have data in both
    if (s_get_cache_value(self, name, AMBIENT_LOCATION_TYPE_HUMIDITY, result)) {
        s_get_cache_value(self, name, AMBIENT_LOCATION_TYPE_TEMP, result);
        return result;
    }

    // not a sensor, must be a location
    if (self->m_list_contents.count(name) == 0) { // should not happen
        return result;
    }

    int outtemp_n = 0;
    int outhum_n  = 0;
    int intemp_n  = 0;
    int inhum_n   = 0;
    result.in_temperature.value  = 0;
    result.out_temperature.value = 0;
    result.in_humidity.value     = 0;
    result.out_humidity.value    = 0;

    std::vector<std::string> content_list = self->m_list_contents.at(name);
    for (auto& content : content_list) {
        //log_trace("content: %s/%s", name.c_str(), content.c_str());
        ambient_values_t result_temp = s_compute_values(self, content); // recursive
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

    bool is_rack = (name.find("rack-") == 0);
    bool is_row  = (name.find("row-")  == 0);

    if (outtemp_n == 0) {
        result.out_temperature.value = std::nan("");
    } else {
        result.out_temperature.value = result.out_temperature.value / outtemp_n;
        if (is_rack || is_row)
            s_publish_value("average.temperature-output", "C", name, result.out_temperature.value, result.out_temperature.ttl);
    }

    if (outhum_n == 0) {
        result.out_humidity.value = std::nan("");
    } else {
        result.out_humidity.value = result.out_humidity.value / outhum_n;
        if (is_rack || is_row)
            s_publish_value("average.humidity-output", "%", name, result.out_humidity.value, result.out_humidity.ttl);
    }

    if (intemp_n == 0) {
        result.in_temperature.value = std::nan("");
    } else {
        result.in_temperature.value = result.in_temperature.value / intemp_n;
        if (is_rack || is_row)
            s_publish_value("average.temperature-input", "C", name, result.in_temperature.value, result.in_temperature.ttl);
    }

    if (inhum_n == 0) {
        result.in_humidity.value = std::nan("");
    } else {
        result.in_humidity.value = result.in_humidity.value / inhum_n;
        if (is_rack || is_row)
            s_publish_value("average.humidity-input", "%", name, result.in_humidity.value, result.in_humidity.ttl);
    }

    if (!is_rack) { // any location, except rack
        double humidity      = 0;
        double temperature   = 0;
        int    n_humidity    = 0;
        int    n_temperature = 0;

        if (!std::isnan(result.out_humidity.value)) {
            n_humidity++;
            humidity += result.out_humidity.value;
        }
        if (!std::isnan(result.in_humidity.value)) {
            n_humidity++;
            humidity += result.in_humidity.value;
            if (result.out_humidity.ttl == 0)
                result.out_humidity.ttl = result.in_humidity.ttl;
        }
        if (!std::isnan(result.out_temperature.value)) {
            n_temperature++;
            temperature += result.out_temperature.value;
        }
        if (!std::isnan(result.in_temperature.value)) {
            n_temperature++;
            temperature += result.in_temperature.value;
            if (result.out_temperature.ttl == 0)
                result.out_temperature.ttl = result.in_temperature.ttl;
        }

        if (n_humidity == 0) {
            result.out_humidity.value = std::nan("");
        } else {
            result.out_humidity.value = humidity / n_humidity;
            s_publish_value("average.humidity", "%", name, result.out_humidity.value, result.out_humidity.ttl);
        }

        if (n_temperature == 0) {
            result.out_temperature.value = std::nan("");
        } else {
            result.out_temperature.value = temperature / n_temperature;
            s_publish_value("average.temperature", "C", name, result.out_temperature.value, result.out_temperature.ttl);
        }
    }

    return result;
}

static int s_remove_asset(AmbientLocation* self, fty_proto_t* bmsg)
{
    assert(self);
    const char* name = fty_proto_name(bmsg);
    log_debug("REMOVE ASSET %s", name);
    //fty_proto_print(bmsg);

    if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_TYPE, ""), "datacenter")) {
        // rm asset from datacenters
        for (unsigned int i = 0; i < self->datacenters.size(); i++) {
            if (self->datacenters[i] == name) {
                self->datacenters.erase(self->datacenters.begin() + i);
                log_trace("datacenters, rm %s", name);
                return 0; // complete
            }
        }
    }
    else {
        // rm asset from containers & contents
        auto it = self->containers.find(name);
        if (it != self->containers.end()) {
            std::string container = it->second;
            self->containers.erase(name); // /!\ here it becomes invalid

            auto contents = self->m_list_contents.find(container);
            if (contents != self->m_list_contents.end()) {
                for (unsigned int i = 0; i < contents->second.size(); i++) {
                    if (contents->second[i] == name) {
                        contents->second.erase(contents->second.begin() + i);
                        return 0; // complete
                    }
                }
            }
        }
    }

    return -1; // asset not handled
}

// returns 0 if ok, else <0
static int s_create_asset(AmbientLocation* self, fty_proto_t* bmsg)
{
    assert(self);
    const char* name = fty_proto_name(bmsg);
    log_debug("CREATE ASSET %s", name);

    if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_TYPE, ""), "datacenter")) {
        log_trace("datacenters, add %s", name);
        self->datacenters.push_back(name);
    }
    else {
        std::string parent;
        if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_SUBTYPE, ""), "sensor")) {
            parent = fty_proto_ext_string(bmsg, "logical_asset", "");
        } else {
            parent = fty_proto_aux_string(bmsg, "parent_name.1", "");
        }
        if (parent == "") { // should never happen
            log_error("parent of '%s' is empty/undefined", name);
            return -1;
        }

        log_trace("containers[%s] = %s", name, parent.c_str());
        self->containers[name] = parent;

        log_trace("m_list_contents[%s] += %s", parent.c_str(), name);
        auto list = self->m_list_contents.find(parent);
        if (list == self->m_list_contents.end()) {
            self->m_list_contents[parent] = { name };
        } else {
            list->second.push_back(name);
        }
    }

    return 0;
}

// handle stream deliver notifications
static void s_handle_actor_stream(AmbientLocation* self, zmsg_t** message_p)
{
    assert(self);
    assert(message_p && (*message_p));

    fty_proto_t* bmsg = fty_proto_decode(message_p);
    if (!bmsg) {
        log_error("Get a stream message that is not fty_proto typed");
        return;
    }

    if (streq(mlm_client_address(self->client), FTY_PROTO_STREAM_METRICS_SENSOR)) {
        // should be a metric here
        if (fty_proto_id(bmsg) != FTY_PROTO_METRIC) {
            log_debug("Get a stream message that is not a metric");
            fty_proto_destroy(&bmsg);
            return;
        }

        // get sensor name
        std::string sensor_name = fty_proto_aux_string(bmsg, "sname", "");
        // get type
        auto s_type = fty_proto_type(bmsg);
        if (!s_type) {
            fty_proto_destroy(&bmsg);
            log_error("Get a stream message that has no type: %s", sensor_name.c_str());
            return;
        }
        std::string type = s_type;

        log_debug("METRIC SENSOR message (asset: %s, type: %s)", sensor_name.c_str(), type.c_str());

        mtx_ambient_hashmap.lock();
        bool metric_in_cache = false;
        if (self->cache.count(sensor_name) != 0) {
            if (type.find("humidity") != std::string::npos) {
                s_remove_from_cache(self, sensor_name, AMBIENT_LOCATION_TYPE_HUMIDITY);
                self->cache.at(sensor_name).second.first = fty_proto_dup(bmsg);
                metric_in_cache = true;
            }
            else if (type.find("temperature") != std::string::npos) {
                s_remove_from_cache(self, sensor_name, AMBIENT_LOCATION_TYPE_TEMP);
                self->cache.at(sensor_name).second.second = fty_proto_dup(bmsg);
                metric_in_cache = true;
            }
        }
        mtx_ambient_hashmap.unlock();

        // PQSWMBT-3723: if sensor metric is handled, publish it in shared memory.
        // metric (or quantity) ex.: 'humidity.default@sensor-241', 'temperature.default@sensor-372'
        if (metric_in_cache) {
            const char* value_s = fty_proto_value(bmsg);
            double value = 0;
            int r = sscanf((value_s ? value_s : ""), "%lf", &value);
            if (r != 1) {
                log_error("parse sensor float value failed (%s/%s, value: '%s')", sensor_name, type.c_str(), value_s);
            } else {
                // here, sensor metric type is like 'temperature.N' or 'humidity.N'
                // where N is the index (offset 0) related to its device owner (epdu, ups, ...).
                // we normalize the metric quantity to 'default'.
                std::string newType;
                if (type.find("temperature") != std::string::npos) {
                    newType = "temperature.default";
                } else if (type.find("humidity") != std::string::npos) {
                    newType = "humidity.default";
                } else {
                    log_debug("type '%s' not handled", type.c_str());
                }
                if (!newType.empty()) {
                    std::string unit = fty_proto_unit(bmsg) ? fty_proto_unit(bmsg) : "";
                    s_publish_value(newType, unit, sensor_name, value, int(fty_proto_ttl(bmsg)));
                }
            }
        }
        // end PQSWMBT-3723
    }
    else if (fty_proto_id(bmsg) == FTY_PROTO_ASSET) {
        if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_TYPE, ""), "device")
            && !streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_SUBTYPE, ""), "sensor")
        ){
            // we are only interested by containers and sensor.
            //log_debug("PROTO ASSET message ignored (asset: '%s', type: '%s')", fty_proto_name(bmsg),
            //    fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_TYPE, ""));

            fty_proto_destroy(&bmsg);
            return;
        }

        log_debug("PROTO ASSET message (%s, op.: %s, status: %s)",
            fty_proto_name(bmsg),
            fty_proto_operation(bmsg),
            fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_STATUS, NULL));

        mtx_ambient_hashmap.lock();
        if (streq(fty_proto_operation(bmsg), FTY_PROTO_ASSET_OP_DELETE)
            || !streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_STATUS, "active"), "active")
        ){
            s_remove_asset(self, bmsg);
        }
        else if (streq(fty_proto_operation(bmsg), FTY_PROTO_ASSET_OP_CREATE)
                 || streq(fty_proto_operation(bmsg), FTY_PROTO_ASSET_OP_UPDATE)
        ){
            s_remove_asset(self, bmsg); // eg. recreate the asset

            int r = s_create_asset(self, bmsg);
            if (r != 0) {
                log_error("s_create_asset failed (%s)", fty_proto_name(bmsg));
            }
            else {
                // add/update sensor in cache
                if (streq(fty_proto_aux_string(bmsg, FTY_PROTO_ASSET_SUBTYPE, ""), "sensor")) {
                    const char* name = fty_proto_name(bmsg);
                    const char* sensor_function = fty_proto_ext_string(bmsg, "sensor_function", "");

                    auto sensor = self->cache.find(name);
                    if (sensor != self->cache.end()) {
                        log_debug("update cache (%s, function: %s)", name, sensor_function);
                        sensor->second.first = sensor_function;
                    }
                    else {
                        log_debug("add in cache (%s, function: %s)", name, sensor_function);
                        self->cache[name]; // new entry
                        sensor = self->cache.find(name);
                        assert(sensor != self->cache.end());
                        sensor->second.first = sensor_function;
                        sensor->second.second.first  = nullptr;
                        sensor->second.second.second = nullptr;
                    }
                }
            }
        }
        mtx_ambient_hashmap.unlock();
    }
    else {
        log_debug("Get a stream message from %s (unhandled)", mlm_client_address(self->client));
    }
    fty_proto_destroy(&bmsg);
}

static void s_ambient_location_calculation(zsock_t* pipe, void* args)
{
    AmbientLocation* self = reinterpret_cast<AmbientLocation*>(args);
    assert(self);

    zpoller_t* poller = zpoller_new(pipe, nullptr);
    assert(poller);

    zsock_signal(pipe, 0);

    log_info("ambient_location_calculation actor: Started");

    while (!zsys_interrupted) {
        int timeout_ms = fty_get_polling_interval() * 1000;
        void* which = zpoller_wait(poller, timeout_ms);

        if (which == nullptr) {
            if (zpoller_terminated(poller) || zsys_interrupted) {
                //log_info("Terminated");
                break;
            }

            // time to calculate, we want to be consistent for each datacenters
            log_info("calculation ticking...");
            mtx_ambient_hashmap.lock();
            for (auto& datacenter : self->datacenters) {
                s_compute_values(self, datacenter);
            }
            mtx_ambient_hashmap.unlock();
            log_debug("calculation ended");
        }
        else if (which == pipe) {
            zmsg_t* msg = zmsg_recv(pipe);
            if (!msg) {
                log_error("pipe recv NULL msg");
                break;
            }

            char* command = zmsg_popstr(msg);
            if (command && streq(command, "$TERM")) {
                log_debug("Got $TERM");
                zmsg_destroy(&msg);
                zstr_free(&command);
                break;
            }

            log_debug("Unknow command '%s'", command);
            zmsg_destroy(&msg);
            zstr_free(&command);
        }
    }

    zpoller_destroy(&poller);

    log_info("ambient_location_calculation actor: Ended");
}

// --------------------------------------------------------------------------
// Create a new fty_ambient_location_server
void fty_ambient_location_server(zsock_t* pipe, void* /*args*/)
{
    AmbientLocation* self = new AmbientLocation();
    assert(self);

    zpoller_t* poller = zpoller_new(pipe, mlm_client_msgpipe(self->client), nullptr);
    assert(poller);

    zsock_signal(pipe, 0);

    log_info("fty_ambient_location_server: Started");

    while (!zsys_interrupted) {
        int timeout_ms = fty_get_polling_interval() * 1000;
        void* which = zpoller_wait(poller, timeout_ms);

        if (which == nullptr) {
            if (zpoller_terminated(poller) || zsys_interrupted) {
                break;
            }
        }
        else if (which == pipe) {
            zmsg_t* msg = zmsg_recv(pipe);
            int r = msg ? s_handle_actor_commands(self, &msg) : 0;
            zmsg_destroy(&msg);
            if (r == 1) {
                break; //$TERM
            }
        }
        else if (which == mlm_client_msgpipe(self->client)) {
            zmsg_t* msg = mlm_client_recv(self->client);
            if (msg && fty_proto_is(msg)) {
                s_handle_actor_stream(self, &msg);
            }
            zmsg_destroy(&msg);
        }
    }

    zpoller_destroy(&poller);
    delete self;

    log_info("fty_ambient_location_server: Ended");
}
