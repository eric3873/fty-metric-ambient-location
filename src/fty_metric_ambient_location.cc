/*  =========================================================================
    fty_metric_ambient_location - Metrics calculator

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

/*
@header
    fty_metric_ambient_location - Metrics calculator
@discuss
@end
*/

#include "fty_ambient_location_server.h"
#include <fty_proto.h>
#include <fty_log.h>

#define AGENT_NAME "fty-metric-ambient-location"
#define MLM_ENDPOINT "ipc://@/malamute"

int main (int argc, char *argv [])
{
    ftylog_setInstance(AGENT_NAME, FTY_COMMON_LOGGING_DEFAULT_CFG);

    bool verbose = false;
    int argn;
    for (argn = 1; argn < argc; argn++) {
        if (streq (argv [argn], "--help")
        ||  streq (argv [argn], "-h")) {
            puts ("fty-metric-ambient-location [options] ...");
            puts ("  --verbose / -v         verbose test output");
            puts ("  --help / -h            this information");
            return 0;
        }
        else
        if (streq (argv [argn], "--verbose")
        ||  streq (argv [argn], "-v"))
            verbose = true;
        else {
            printf ("Unknown option: %s\n", argv [argn]);
            return 1;
        }
    }

    if (verbose) {
        ftylog_setVerboseMode(ftylog_getInstance());
    }

    log_info ("%s - starting...", AGENT_NAME);

    zactor_t *server = zactor_new (fty_ambient_location_server, NULL);
    if (!server) {
        log_error("crate server failed");
        return EXIT_FAILURE;
    }

    zstr_sendx (server, "CONNECT", MLM_ENDPOINT, AGENT_NAME, NULL);
    zstr_sendx (server, "CONSUMER", FTY_PROTO_STREAM_METRICS_SENSOR, ".*", NULL);
    zstr_sendx (server, "CONSUMER", FTY_PROTO_STREAM_ASSETS, ".*", NULL);
    zstr_sendx (server, "START", NULL);

    log_info ("%s - started", AGENT_NAME);

    // Main loop, accept any message back from server
    // copy from src/malamute.c under MPL license
    while (!zsys_interrupted) {
        char* msg = zstr_recv(server);
        if (!msg)
            break;
        log_trace("%s: recv msg '%s'", AGENT_NAME, msg);
        zstr_free(&msg);
    }

    zactor_destroy (&server);

    log_info ("%s - ended", AGENT_NAME);

    return EXIT_SUCCESS;
}
