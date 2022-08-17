<?php

namespace Kafka;

use Illuminate\Database\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    public function connect(array $config)
    {
        $conf = new \Rdkafka\Conf();

        $conf->set('bootstrap.servers', $config['bootstrap.servers']);
        $conf->set('security.protocol', $config['security.protocol']);
        $conf->set('sasl.mechanisms', $config['sasl.mechanisms']);
        $conf->set('sasl.username',  $config['sasl.username']);
        $conf->set('sasl.password',  $config['sasl.password']);
        // $conf->set('log_level', (string) LOG_DEBUG);
        // $conf->set('debug', 'msg');

        $producer = new \RdKafka\Producer($conf);

        $conf->set('group.id', $config['group.id']);
        $conf->set('auto.offset.reset', 'earliest');
        
        $consumer = new \RdKafka\KafkaConsumer($conf);

        return new KafkaQueue($producer, $consumer);
    }

}