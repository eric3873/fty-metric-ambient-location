# fty-metric-ambient-location
it compute the average humidity and temperature of each locations, based on sensor metrics.

The default configuration values are in fty-ambient-location.cfg file (section default)

## How to build
To build fty-metric-ambient-location project run:
```bash
mkdir build && cd build
cmake -DCMAKE_BUILD_TYPE=Debug -DCMAKE_INSTALL_PREFIX=usr -DBUILD_TESTING=On ..
make
sudo make install
```
## How to run

To run fty-metric-ambient-location project:
#### from within the source tree, run:
```bash
./src/fty-metric-ambient-location
```

#### from an installed base, using systemd, run:

```bash
systemctl start fty-metric-ambient-location
```
## Protocols

fty-ambient-location suscribe to ASSETS stream in order to build a hierarchy of containers.
It also suscribe to _METRICS_SENSOR  _to get the data.

It will publish calculated metrics on shm.
Example of metrics name : average.humidity-input@rack-32
average.humidity@row-12

## Sensor propagation in physical topology

Sensors are propagated trough the physical topology of the logical asset to the upper levels.

Let's explain it on the example. We have a physical topology: 
```
DC01 -> ROOM01 -> ROW01 |-> RACK01 -> UPS01  -> SENSOR01 
                        |-> RACK02 -> EPDU01 -> SENSOR02
```

SENSOR01 is logically assigned to RACK01 with function 'input' and SENSOR02 is logically assigned to RACK02 with function 'output'.

What would be computed:

* RACK01: "input" T&H will take in account only SENSOR01
* RACK01: "output" T&H nothing, as there is no sensor assigned with such function
* RACK02: "input" T&H nothing, as there is no sensor assigned with such function
* RACK02: "output" T&H will take in account only SENSOR02
* ROW01: There is no sensor logically assigned to this row, it will use the computed average of RACK01 and RACK02. The rows will compute averages for each sensor function and a global average of SENSOR01, SENSOR02.
* ROOM01: There is no sensor logically assigned to this room, it will use the computed average of ROW01. As for rooms we do not distinguish sensor function, the T&H would be computed as average of SENSOR01, SENSOR02.
* DC01: There is no sensor logically assigned to this dc, it will use the computed average of ROOM01. As for dcs we do not distinguish sensor function, the T&H would be computed as average of SENSOR01, SENSOR02.