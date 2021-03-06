## loopback-connector-oracle

The Oracle Connector module for for [loopback-datasource-juggler](https://github.com/strongloop/loopback-datasource-juggler).

For full documentation, see the official [StrongLoop  Documentation](http://docs.strongloop.com/display/LB/Oracle+connector).

**NOTE**: This connector is compatible with Node v.0.10.x, v.12.x, and iojs v1.6.2.  

## Installation

To simplify the installation of [node-oracle](https://github.com/strongloop/node-oracle) module and Oracle instant clients,
we introduce [loopback-oracle-installer](https://github.com/strongloop/loopback-oracle-installer) as a dependency which installs
and configures node-oracle upon `npm install`.

Please note `config.oracleUrl` is the property to define the base URL to download the corresponding node-oracle bundle for the local
environment.

The bundle file name is `loopback-oracle-<platform>-<arch>-<version>.tar.gz`. The `version` is the same as the `version` in package.json.

    "dependencies": {
        "loopback-oracle-installer": "git+ssh://git@github.com:strongloop/loopback-oracle-installer.git",
             ...
    },
    "config": {
        "oracleUrl": "http://7e9918db41dd01dbf98e-ec15952f71452bc0809d79c86f5751b6.r22.cf1.rackcdn.com"
    },

**The `oracleUrl` can be overridden via LOOPBACK_ORACLE_URL environment variable.**

For MacOSX, the full URL is:

http://7e9918db41dd01dbf98e-ec15952f71452bc0809d79c86f5751b6.r22.cf1.rackcdn.com/loopback-oracle-MacOSX-x64-0.0.1.tar.gz

`libaio` library is required on Linux systems:

* On Ubuntu/Debian

        ﻿sudo apt-get install libaio1

* On Fedora/CentOS/RHEL

        ﻿sudo yum install libaio


**Please make sure c:\instantclient_12_1\vc10 comes before c:\instantclient_12_1**

## Examples

* example/app.js: Demonstrate the asynchronous discovery
* example/app-sync.js: Demonstrate the synchronous discovery

## Running tests

The tests in this repository are mainly integration tests, meaning you will need
to run them using our preconfigured test server.

1. Ask a core developer for instructions on how to set up test server
   credentials on your machine
2. `npm test`
