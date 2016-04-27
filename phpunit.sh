#!/usr/bin/env bash

# install dependencies
curl -sS https://getcomposer.org/installer | php
echo "memory_limit = -1" >> /etc/php.ini
php composer.phar install -n;

# run test suite
export ROOT_PATH="/code";
./vendor/bin/phpunit;
