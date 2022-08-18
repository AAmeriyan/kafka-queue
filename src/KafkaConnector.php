<?php

    namespace kafka;

    use App\Queues\KafkaQueue;
    use Illuminate\Queue\Connectors\ConnectorInterface;

    class KafkaConnector implements ConnectorInterface

    {

        public function connect(array $config)
        {
            $conf = new \RdKafka\Conf();
            $conf->set('bootstrap.servers',$config['bootstrap_servers']);
            $conf->set('group.id', $config['group_id']);

            $consumer = new \Rdkafka\KafkaConsumer($conf);
            return new KafkaQueue($consumer);
        }
    }
