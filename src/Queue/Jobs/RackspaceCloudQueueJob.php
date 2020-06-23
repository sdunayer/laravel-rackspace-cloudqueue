<?php

namespace Tailwind\RackspaceCloudQueue\Queue\Jobs;

use Illuminate\Container\Container;
use Illuminate\Queue\Jobs\Job;
use Illuminate\Contracts\Queue\Job as JobContract;

use OpenCloud\Queues\Resource\Queue as OpenCloudQueue;
use OpenCloud\Queues\Resource\Message;

/**
 * Class RackspaceCloudQueueJob
 * @package Tailwind\RackspaceCloudQueue\Queue\Jobs
 */
class RackspaceCloudQueueJob extends Job implements JobContract
{
    /**
     * The Rackspace OpenCloudQueue instance.
     *
     * @var OpenCloudQueue
     */
    protected $openCloudQueue;

    /**
     * The message instance.
     *
     * @var Message
     */
    protected $message;

    /**
     * @param Container      $container
     * @param OpenCloudQueue $openCloudQueue
     * @param string         $queue
     * @param Message        $message
     * @param string         $connectionName
     */
    public function __construct(Container $container, OpenCloudQueue $openCloudQueue, $queue, Message $message, string $connectionName)
    {
        $this->openCloudQueue = $openCloudQueue;
        $this->message        = $message;
        $this->queue          = $queue;
        $this->container      = $container;
        $this->connectionName = $connectionName;
    }

    /**
     * Release the job back into the queue.
     *
     * @param  int $delay
     * @return void
     */
    public function release($delay = 0)
    {
        $this->message->delete($this->message->getClaimIdFromHref());
    }

    /**
     * Get the number of times the job has been attempted.
     *
     * @throws RuntimeException
     */
    public function attempts()
    {
        return 1;
        throw new RuntimeException('RackspaceCloudQueueJob::attempts() is unsupported');
    }

    /**
     * Get the raw body string for the job.
     *
     * @return string
     */
    public function getRawBody()
    {
        return $this->message->getBody();
    }

    /**
     * Delete the job from the queue.
     *
     * @return void
     */
    public function delete()
    {
        parent::delete();

        $this->openCloudQueue->deleteMessages(array($this->message->getId()));
    }


    /**
     * Get the job identifier.
     *
     * @return string
     */
    public function getJobId()
    {
        return $this->message->getId();
    }
}
