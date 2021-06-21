cmake_minimum_required(VERSION 3.13)
cmake_policy(VERSION 3.13)

project(fty-metric-ambient-location
    VERSION 1.0.0
    DESCRIPTION "Compute the ambient metrics of each location, based on their sensors metrics"
)

##############################################################################################################
find_package(fty-cmake PATHS ${CMAKE_BINARY_DIR}/fty-cmake)
##############################################################################################################

etn_target(static ${PROJECT_NAME}-lib
    SOURCES
        src/fty_ambient_location_server.cc
        src/fty_ambient_location_server.h
    USES
        czmq
        fty_common_logging
        fty_proto
        fty_shm
        mlm
    PRIVATE
)

##############################################################################################################

etn_target(exe ${PROJECT_NAME}
    SOURCES
        src/fty_metric_ambient_location.cc
    USES
        ${PROJECT_NAME}-lib
)

##############################################################################################################

etn_test_target(${PROJECT_NAME}-lib
    SOURCES
        tests/main.cpp
        tests/ambient_location_server.cpp
    PREPROCESSOR
        -DCATCH_CONFIG_FAST_COMPILE
    SUBDIR
        tests
)

##############################################################################################################

#install resources files
set(MAL_SETTINGS_DIR "${CMAKE_INSTALL_FULL_LOCALSTATEDIR}/lib/fty/${PROJECT_NAME}")
set(MAL_USER "bios")

configure_file("${PROJECT_SOURCE_DIR}/resources/fty-metric-ambient-location.cfg.in" "${PROJECT_BINARY_DIR}/resources/fty-metric-ambient-location.cfg" @ONLY)
install(FILES "${PROJECT_BINARY_DIR}/resources/fty-metric-ambient-location.cfg" DESTINATION ${CMAKE_INSTALL_FULL_SYSCONFDIR}/${PROJECT_NAME})

configure_file("${PROJECT_SOURCE_DIR}/resources/fty-metric-ambient-location.service.in" "${PROJECT_BINARY_DIR}/resources/fty-metric-ambient-location.service" @ONLY)
install(FILES "${PROJECT_BINARY_DIR}/resources/fty-metric-ambient-location.service" DESTINATION ${CMAKE_INSTALL_PREFIX}/lib/systemd/system/)
