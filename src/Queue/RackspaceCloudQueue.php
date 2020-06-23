<?php

namespace Tailwind\RackspaceCloudQueue\Queue;

use Illuminate\Contracts\Queue\Queue as QueueContract;
use Illuminate\Queue\Queue;
use Illuminate\Support\Str;
use Illuminate\Support\Arr;
use RuntimeException;

use OpenCloud\Common\Constants\Datetime;
use OpenCloud\Queues\Resource\Queue as OpenCloudQueue;
use OpenCloud\Queues\Service as OpenCloudService;

use Tailwind\RackspaceCloudQueue\Queue\Jobs\RackspaceCloudQueueJob;

/**
 * Class RackspaceCloudQueue
 * @package Tailwind\RackspaceCloudQueue\Queue
 */
class RackspaceCloudQueue extends Queue implements QueueContract
{
    /**
     * The Rackspace OpenCloud Message Service instance.
     *
     * @var OpenCloudService
     */
    protected $openCloudService;

    /**
     * The name of the default queue.
     *
     * @var string
     */
    protected $default;

    /**
     * The Rackspace OpenCloud Queue instance
     *
     * @var OpenCloudQueue
     */

    protected $queue;

    /**
     * @param  OpenCloudService $openCloudService
     * @param  string  $default
     * @return void
     * @throws OpenCloud\Common\Exceptions\InvalidArgumentError
     */
    public function  __construct(OpenCloudService $openCloudService, $default = 'default')
    {
        $this->openCloudService = $openCloudService;
        $this->default          = $default;
        $this->queue            = $this->getQueue($default);
    }

    /**
     * Get the size of the queue.
     *
     * @param  string|null  $queue
     * @return int
     */
    public function size($queue = null)
    {
        $cloudQueue = $this->getQueue($queue);
        $queueStats = $cloudQueue->getStats();

        return $queueStats !== false ? $queueStats->messages->total : 0;
    }


    /**
     * Push a new job onto the queue.
     *
     * @param  string $job
     * @param  mixed  $data
     * @param  string $queue
     * @return bool
     */
    public function push($job, $data = '', $queue = null)
    {
        return $this->pushRaw($this->createPayload($job, $data), $queue);
    }

    /**
     * Push a raw payload onto the queue.
     *
     * @param  string $payload
     * @param  string $queue
     * @param  array  $options
     * @return bool
     */
    public function pushRaw($payload, $queue = null, array $options = array())
    {
        $ttl = Arr::get($options, 'ttl', Datetime::DAY * 2);

        $cloudQueue = $this->getQueue($queue);

        return $cloudQueue->createMessage(
            array(
                'body' => $payload,
                'ttl'  => $ttl,
            )
        );
    }

    /**
     * Push a new job onto the queue after a delay.
     *
     * @param  DateTime|int $delay
     * @param  string        $job
     * @param  string        $data
     * @param  null          $queue
     * @return mixed|void
     */
    public function later($delay, $job, $data = '', $queue = null)
    {
        throw new RuntimeException('RackspaceCloudQueue::later() method is not supported');
    }

    /**
     * Pop the next job off of the queue.
     *
     * @param  string $queue
     * @return Illuminate\Queue\Jobs\Job|null|RackspaceCloudQueueJob
     */
    public function pop($queue = null)
    {
        $cloudQueue = $this->getQueue($queue);

        /**
         * @var OpenCloud\Common\Collection\PaginatedIterator $response
         */
        $response = $cloudQueue->claimMessages(
            array(
                'limit' => 1,
                'grace' => 5 * Datetime::MINUTE,
                'ttl'   => 5 * Datetime::MINUTE,
            ));

        if ($response and $response->valid()) {
            $message = $response->current();

            return new RackspaceCloudQueueJob($this->container, $cloudQueue, $queue, $message, $this->connectionName);
        }
    }

    /**
     * Get the queue or return the default.
     * @param  $queue
     * @return OpenCloudQueue
     */
    protected function getQueue($queue)
    {
        return is_null($queue) ? $this->queue : $this->openCloudService->createQueue($queue);
    }
}
