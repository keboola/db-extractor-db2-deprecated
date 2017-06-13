FROM php:7.1
MAINTAINER Miro Cillik <miro@keboola.com>

# Install dependencies
RUN apt-get update && apt-get install -y \
    apt-utils \
    ksh \
    zip \
    unzip \
    unixodbc-dev \
    curl

# Install PHP odbc extension
RUN set -x \
    && docker-php-source extract \
    && cd /usr/src/php/ext/odbc \
    && phpize \
    && sed -ri 's@^ *test +"\$PHP_.*" *= *"no" *&& *PHP_.*=yes *$@#&@g' configure \
    && ./configure --with-unixODBC=shared,/usr \
    && docker-php-ext-install odbc \
    && docker-php-source delete

# Install DB2 Client
RUN mkdir -p /opt/ibm
WORKDIR /opt/ibm
#ADD driver/ibm_data_server_driver_package_linuxx64_v10.5.tar.gz /opt/ibm/
ADD driver/v10.5fp7_linuxx64_dsdriver.tar.gz /opt/ibm/

RUN ksh dsdriver/installDSDriver
ENV IBM_DB_HOME /opt/ibm/dsdriver

# Install ibm_db2 and pdo_odbc PHP extensions
RUN echo $IBM_DB_HOME | pecl install ibm_db2
RUN docker-php-ext-enable ibm_db2
RUN docker-php-ext-configure pdo_odbc --with-pdo-odbc=ibm-db2,/opt/ibm/dsdriver
RUN docker-php-ext-install pdo_odbc
RUN export LD_LIBRARY_PATH=$IBM_DB_HOME/lib

# Install Composer dependencies
ADD . /code
WORKDIR /code
RUN curl -sS https://getcomposer.org/installer | php
RUN echo "memory_limit = -1" >> /etc/php.ini
RUN php composer.phar install --no-interaction

# Run app
CMD php ./run.php --data=/data
