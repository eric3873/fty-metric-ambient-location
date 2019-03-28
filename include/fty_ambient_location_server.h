/*  =========================================================================
    fty_ambient_location_server - Ambient location metrics server

    Copyright (C) 2014 - 2019 Eaton

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

#ifndef FTY_AMBIENT_LOCATION_SERVER_H_INCLUDED
#define FTY_AMBIENT_LOCATION_SERVER_H_INCLUDED

#ifdef __cplusplus
class AmbientLocation{
  public :
    AmbientLocation ();
    ~AmbientLocation();
    int timeout_ms;
    mlm_client_t *client;
    zactor_t *ambient_calculation;
    std::unordered_map <std::string, std::string> containers;
    std::unordered_map <std::string, std::vector<std::string>> m_list_contents;
    std::unordered_map <std::string, std::pair<std::string, std::pair<fty_proto_t*, fty_proto_t*>>> cache;
    std::vector<std::string> datacenters;   
};

//  @interface
FTY_METRIC_AMBIENT_LOCATION_EXPORT void
    ambient_location_calculation (zsock_t *pipe, void *args);

//  Self test of this class
FTY_METRIC_AMBIENT_LOCATION_EXPORT void
    fty_ambient_location_server_test (bool verbose);

FTY_METRIC_AMBIENT_LOCATION_EXPORT void
    fty_ambient_location_server (zsock_t *pipe, void *args);

//  @end
extern "C" {
#endif
#ifdef __cplusplus
}
#endif

#endif
