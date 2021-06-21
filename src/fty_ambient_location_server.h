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

#pragma once
#include <czmq.h>
#include <fty_proto.h>
#include <malamute.h>
#include <string>
#include <unordered_map>
#include <vector>

class AmbientLocation
{
public:
    AmbientLocation();
    ~AmbientLocation();

    using Containers     = std::unordered_map<std::string, std::string>;
    using ContainersList = std::unordered_map<std::string, std::vector<std::string>>;
    using Cache       = std::unordered_map<std::string, std::pair<std::string, std::pair<fty_proto_t*, fty_proto_t*>>>;
    using Datacenters = std::vector<std::string>;

    int            timeout_ms          = {};
    mlm_client_t*  client              = nullptr;
    zactor_t*      ambient_calculation = nullptr;
    Containers     containers;
    ContainersList m_list_contents;
    Cache          cache;
    Datacenters    datacenters;
};

void ambient_location_calculation(zsock_t* pipe, void* args);
void fty_ambient_location_server(zsock_t* pipe, void* args);
