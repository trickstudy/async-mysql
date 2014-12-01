<?php

namespace Atrox;

use React\Promise;
use React\Promise\Deferred;
use React\EventLoop\LoopInterface;


class AsyncMysql {

  private $loop;
  private $pool;


  /**
   * @param React\EventLoop\LoopInterface
   * @param Atrox\ConnectionPool|callable either connection pool or function that makes connection
   */
  function __construct(LoopInterface $loop, $connectionPool, $maxConnections = 100) {
    $this->loop = $loop;
    $this->pool = ($connectionPool instanceof ConnectionPool) ? $connectionPool : new ConnectionPool($connectionPool, $maxConnections);
  }


  /**
   * @param string
   * @return React\Promise\PromiseInterface
   */
  function query($query) {
    return $this->pool->getConnection()->then(function ($conn) use ($query) {  // getConnection returns a promise, so we use then to process it
      $status = $conn->query($query, MYSQLI_ASYNC);  // Send the query to MySQL specifying async
      if ($status === false) {  // Means the query failed
        $this->pool->freeConnection($conn);  // Release the connection back to the pool
        throw new \Exception($conn->error);  // Throw a PHP Exception with the MySQL error
      }

      $defered = new Deferred();  // Create the deferred to return to our caller

      $this->loop->addPeriodicTimer(0.001, function ($timer) use ($conn, $defered) {  // Tell React to run the below every 0.001 seconds
        $links = $errors = $reject = array($conn);  // Initialize some vars
        mysqli_poll($links, $errors, $reject, 0);  // Poll for results, now, and at the top of every execution of this periodic function
        if (($read = in_array($conn, $links, true))  // Our connection appearing in the $links array means we have results waiting 
          || ($err = in_array($conn, $errors, true))  // Our connection appearing in the $error array means we have errors
          || ($rej = in_array($conn, $reject, true))) {  // Our connection appearing in the $reject array means our query was rejected
          if ($read) {  // If we have results to read
            $result = $conn->reap_async_query();  // Load the query results from the connection with them waiting ($conn)
            if ($result === false) {  // Means the provided results indicate a query error
              $defered->reject(new \Exception($conn->error));  // Throw a PHP Excpetion with said error
            } else {  // Otherwise we have "good" results
              if(stripos($query, "INSERT") === 0) {  // If it is an insert query
                $result = $conn->last_id;  // set the result to the inserted ID instead of affected rows
              }
              $defered->resolve($result);  // So resolve the deferred with the query results
            }
          } elseif ($err) {  // If we found errors in out polling...
            $defered->reject($conn->error);  // reject the deferred with those errors
          } else {  // In all other cases
            $defered->reject(new \Exception('Query was rejected'));  // reject the deferred indicating the MySQL rejection
          }
          $timer->cancel();  // We have results, rejection, or errors to stop checking every 0.001
          $this->pool->freeConnection($conn);  // Release the connection back into the pool
        }
      });

      return $defered->promise();  // Return our promise to the caller
    });
  }
}



class ConnectionPool {

  private $makeConnection;
  private $maxConnections;

  /** pool of all connections (both idle and busy) */
  private $pool;

  /** pool of idle connections */
  private $idle;

  /** array of Deferred objects waiting to be resolved with connection */
  private $waiting = [];


  function __construct($makeConnection, $maxConnections = 100) {
    $this->makeConnection = $makeConnection;
    $this->maxConnections = $maxConnections;
    $this->pool = new \SplObjectStorage();
    $this->idle = new \SplObjectStorage();
  }


  function getConnection() {
    // reuse idle connections
    if (count($this->idle) > 0) {
      $this->idle->rewind();
      $conn = $this->idle->current();
      $this->idle->detach($conn);
      return Promise\resolve($conn);
    }

    // max connections reached, must wait till one connection is freed
    if (count($this->pool) >= $this->maxConnections) {
      $deferred = new Deferred();
      $this->waiting[] = $deferred;
      return $deferred->promise();
    }

    $conn = call_user_func($this->makeConnection);
    $this->pool->attach($conn);
    return ($conn === false) ? Promise\reject(new \Exception(mysqli_connect_error())) : Promise\resolve($conn);
  }


  function freeConnection(\mysqli $conn) {
    if (!empty($this->waiting)) {
      $deferred = array_shift($this->waiting);
      $deferred->resolve($conn);
    } else {
      return $this->idle->attach($conn);
    }
  }
}
