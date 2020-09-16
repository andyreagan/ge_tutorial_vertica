# Great Expectations tutorial with Vertica

Load data into a Vertica database for the GE tutorial.
Data from the [main tutorial repo](https://github.com/superconductive/ge_tutorials).

Notes here on running this [on Medium](https://medium.com/@andyreagan/great-expectations-of-vertica-99a2c886cd58) and [my blog](https://andyreagan.github.io/2020/05/29/great-expectations-of-vertica/).

The Vertica instance is run via docker:

    docker run -p 5433:5433 -e DATABASE_PASSWORD='foo123' jbfavre/vertica:9.2.0-7_debian-8