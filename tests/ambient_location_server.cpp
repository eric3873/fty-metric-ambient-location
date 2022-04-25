#include "src/fty_ambient_location_server.h"
#include <catch2/catch.hpp>
#include <fty_shm.h>
#include <malamute.h>

//  --------------------------------------------------------------------------
//  Self test of this class

TEST_CASE("ambient location server test")
{
    static const char* endpoint = "inproc://fty_metric_ambient_location_test";

    zactor_t* server = zactor_new(mlm_server, const_cast<char*>("Malamute"));
    zstr_sendx(server, "BIND", endpoint, nullptr);

    const char* SELFTEST_DIR_RW = ".";

    fty_shm_set_test_dir(SELFTEST_DIR_RW);
    fty_shm_set_default_polling_interval(2);

    zactor_t* ambient_location = zactor_new(fty_ambient_location_server, nullptr);

    zstr_sendx(ambient_location, "CONNECT", endpoint, "fty-ambient-location", nullptr);
    zstr_sendx(ambient_location, "CONSUMER", FTY_PROTO_STREAM_METRICS_SENSOR, ".*", nullptr);
    zstr_sendx(ambient_location, "CONSUMER", FTY_PROTO_STREAM_ASSETS, ".*", nullptr);

    sleep(1);
    mlm_client_t* producer_m = mlm_client_new();
    mlm_client_connect(producer_m, endpoint, 1000, "producer_m");
    mlm_client_set_producer(producer_m, FTY_PROTO_STREAM_METRICS_SENSOR);
    mlm_client_t* producer = mlm_client_new();
    mlm_client_connect(producer, endpoint, 1000, "producer");
    mlm_client_set_producer(producer, FTY_PROTO_STREAM_ASSETS);

    // Build hierarchy (two sensor -input- on a datacenter)
    zhash_t* aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "status", const_cast<char*>("active"));
    zhash_insert(aux, "type", const_cast<char*>("device"));
    zhash_insert(aux, "subtype", const_cast<char*>("sensor"));

    zhash_t* ext = zhash_new();
    zhash_autofree(ext);

    zhash_insert(ext, "logical_asset", const_cast<char*>("datacenter-1"));
    zhash_insert(ext, "sensor_function", const_cast<char*>("input"));

    const char* subject = "ASSET_MANIPULATION";
    zmsg_t*     msg     = fty_proto_encode_asset(aux, "sensor-1", FTY_PROTO_ASSET_OP_CREATE, ext);
    int         rv      = mlm_client_send(producer, subject, &msg);
    REQUIRE(rv == 0);

    if (aux)
        zhash_destroy(&aux);
    if (ext)
        zhash_destroy(&ext);

    aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "status", const_cast<char*>("active"));
    zhash_insert(aux, "type", const_cast<char*>("device"));
    zhash_insert(aux, "subtype", const_cast<char*>("sensor"));

    ext = zhash_new();
    zhash_autofree(ext);

    zhash_insert(ext, "logical_asset", const_cast<char*>("datacenter-1"));
    zhash_insert(ext, "sensor_function", const_cast<char*>("input"));

    msg = fty_proto_encode_asset(aux, "sensor-2", FTY_PROTO_ASSET_OP_CREATE, ext);
    rv  = mlm_client_send(producer, subject, &msg);
    REQUIRE(rv == 0);

    if (aux)
        zhash_destroy(&aux);
    if (ext)
        zhash_destroy(&ext);

    aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "status", const_cast<char*>("active"));
    zhash_insert(aux, "type", const_cast<char*>("datacenter"));
    zhash_insert(aux, "subtype", const_cast<char*>("N_A"));

    msg = fty_proto_encode_asset(aux, "datacenter-1", FTY_PROTO_ASSET_OP_CREATE, nullptr);
    rv  = mlm_client_send(producer, subject, &msg);
    REQUIRE(rv == 0);

    if (aux)
        zhash_destroy(&aux);

    sleep(1);
    zstr_sendx(ambient_location, "START", nullptr);
    sleep(1);

    // publish metrics
    aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "sname", const_cast<char*>("sensor-1"));

    msg = fty_proto_encode_metric(aux, uint64_t(time(nullptr)), 60, "humidity.0", "HM1", "40", "%");
    REQUIRE(msg);
    mlm_client_send(producer_m, "humidity.0@HM1", &msg);
    if (aux)
        zhash_destroy(&aux);

    // wait calculation
    sleep(5);

    fty_proto_t* m;
    {
        fty::shm::shmMetrics resultT;
        fty::shm::read_metrics("datacenter-1", ".*humidity", resultT);
        m = resultT.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "40.00")); // <<< 40 / 1
        fty_shm_delete_test_dir();
        fty_shm_set_test_dir(SELFTEST_DIR_RW);
        m = nullptr;
    }

    aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "sname", const_cast<char*>("sensor-2"));

    msg = fty_proto_encode_metric(aux, uint64_t(time(nullptr)), 60, "humidity.0", "HM2", "100", "%");
    REQUIRE(msg);
    mlm_client_send(producer_m, "humidity.0@HM2", &msg);
    if (aux)
        zhash_destroy(&aux);

    // wait calculation
    sleep(5);

    {
        fty::shm::shmMetrics resultT;
        fty::shm::read_metrics("datacenter-1", ".*humidity", resultT);
        m = resultT.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "70.00")); // <<< (100 + 40) / 2
        fty_shm_delete_test_dir();
        fty_shm_set_test_dir(SELFTEST_DIR_RW);
        m = nullptr;
    }

    // send value for HM1 again
    aux = zhash_new();
    zhash_autofree(aux);

    zhash_insert(aux, "sname", const_cast<char*>("sensor-1"));

    msg = fty_proto_encode_metric(aux, uint64_t(time(nullptr)), 60, "humidity.0", "HM1", "70", "%");
    REQUIRE(msg);
    mlm_client_send(producer_m, "humidity.0@HM1", &msg);
    if (aux)
        zhash_destroy(&aux);

    // wait calculation
    sleep(5);

    {
        fty::shm::shmMetrics resultT;
        fty::shm::read_metrics("datacenter-1", ".*humidity", resultT);
        m = resultT.get(0);
        fty_proto_print(m);
        REQUIRE(m);
        CHECK(streq(fty_proto_value(m), "85.00")); // <<< (70 + 100)  / 2
        fty_shm_delete_test_dir();
        fty_shm_set_test_dir(SELFTEST_DIR_RW);
        m = nullptr;
    }

    zactor_destroy(&ambient_location);
    mlm_client_destroy(&producer);
    mlm_client_destroy(&producer_m);
    zactor_destroy(&server);
    fty_shm_delete_test_dir();
}
