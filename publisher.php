<?php

require __DIR__ . '/vendor/autoload.php';
include(__DIR__ . '/config.php');

use PhpAmqpLib\Connection\AMQPStreamConnection;
use PhpAmqpLib\Exchange\AMQPExchangeType;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Wire\AMQPTable;

$exchange = 'stream-test-exchange';
$queue = 'stream-test-queue';

$connection = new AMQPStreamConnection(HOST, PORT, USER, PASS, VHOST);
$channel = $connection->channel();
$channel->basic_qos(0, 10, false);

$amqpTable = new AMQPTable();
$amqpTable->set("x-queue-type", "stream");
$channel->queue_declare(
    $queue,
    false,
    true,
    false,
    false,
    false,
    $amqpTable
);

$channel->exchange_declare(
    $exchange,
    AMQPExchangeType::DIRECT,
    false,
    true,
    false
);

$channel->queue_bind($queue, $exchange);

$sentence = "The quick brown fox jumps over the lazy dog";
$sentence = explode(' ', $sentence);

// publish messages to the queue
for ($i = 0; $i < count($sentence); $i++) {
    $body = array_slice($sentence, 0, $i+1);
    $messageBody = json_encode(
        [
            'id' => 1,
            'body' => implode(' ', $body),
            'updated_at' => new DateTimeImmutable()
        ]
    );
    $message = new AMQPMessage(
        $messageBody,
        array(
            'content_type' => 'application/json',
            'delivery_mode' => AMQPMessage::DELIVERY_MODE_PERSISTENT,
            'timestamp' => time()
        )
    );
    $channel->basic_publish($message, $exchange);
    sleep(1);
}

$channel->close();
$connection->close();