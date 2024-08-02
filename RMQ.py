from pika import channel
from pika import SelectConnection
from pika import frame
from pika import spec
from pika import URLParameters
from queue import Queue
import time
import functools
import threading


class RMQ(threading.Thread):
    """
    This class is a simple RMQ consumer which can obtain queue size and allows consuming of messages.
    To consume messages, you need to provide on_message callback to the class contructor.
    """

    def __init__(
        self,
        queueName: str,
        exchangeName: str,
        routing_key: str = "",
        address: str = "",
        on_message: callable = None,
        on_queue_bind: callable = None
    ):
        threading.Thread.__init__(self)
        self._connection = None
        self._channel = None
        self._queue = queueName
        self._exchange = exchangeName
        self._address = address
        self._exchange_type = "fanout"
        self._on_message_callback = (
            on_message if on_message is not None else self.on_message_pass
        )
        self._on_queue_bind = (
            on_queue_bind if on_queue_bind is not None else self.on_queue_bind
        )
        self._is_connected = False
        self._shutdown = False
        self._routing_key = routing_key
        self._finished = False
        self._reconnect_delay_s = 5
        self._connected_queue = Queue()
        self._shutdown_queue = Queue()

    def on_message_pass(
        self,
        channel: channel.Channel,
        method: spec.Basic.Deliver,
        properties: spec.BasicProperties,
        body: bytes,
    ):
        pass

    def on_open_channel(self, channel: channel.Channel):
        print("Channel setup success")
        self._channel = channel
        self._channel.add_on_close_callback(self.on_channel_closed)
        self._channel.exchange_declare(
            exchange=self._exchange,
            exchange_type=self._exchange_type,
            callback=self.on_exchange_declare,
        )

    def on_exchange_declare(self, unused_frame: frame.Method):
        print("Declared exchange {}".format(self._exchange))
        self._channel.queue_declare(
            queue=self._queue, callback=self.on_queue_declare, durable=True
        )

    def on_queue_declare(self, method_frame: frame.Method):
        print("Declared queue {}".format(self._queue))
        self._channel.queue_bind(
            queue=self._queue,
            exchange=self._exchange,
            routing_key=self._routing_key,
            callback=self._on_queue_bind,
        )
        self._channel.add_on_cancel_callback(self.on_channel_cancelled)
        # Call this if you want to start consuming messages. Note that you need to provied proper on_message callaback
        # self._channel.basic_consume(
        #    on_message_callback=self._on_message_callback,
        #    queue=self._queue)

    def on_channel_cancelled(self, method_frame: frame.Method):
        print("Channel canceled")
        if self._channel:
            self._channel.close()

    def on_queue_bind(self, unused_frame: frame.Method):
        print("Queue {} bound to exchange {}".format(self._queue, self._exchange))
        self._connected_queue.put(None)
        self._connected_queue.task_done()
        self._is_connected = True

    def on_channel_closed(self, channel: channel.Channel, exception: Exception):
        self._is_connected = False
        print("Channel {} is closed".format(channel))
        self._channel = None
        if not self._shutdown:
            print("Closing connection and reconnecting to channel")
            self._connection.close()
        else:
            print("RMQ shutdown - channel")

    def on_open_connection(self, connection: SelectConnection):
        print("Connection opened")
        self._channel = None
        connection.channel(on_open_callback=self.on_open_channel)

    def on_connection_closed(self, connection: SelectConnection, exception: Exception):
        print("Connection closed: {}".format(str(exception)))
        self._channel = None
        if not self._shutdown:
            print("Reopening connection in {} seconds".format(self._reconnect_delay_s))
        else:
            print(f"RMQ shutdown - connection closed for queue {self._queue}")
            self._shutdown_queue.put(None)
            self._shutdown_queue.task_done()
        self._connection.ioloop.stop()

    def getQueueName(self) -> str:
        return self._queue

    def shutdown(self):
        self._shutdown = True
        print(f"Shutting down connection and channel for queue {self._queue}")
        if self._channel is not None:
            print("Closing channel")
            self._channel.close()
        if self._connection is not None:
            print("Closing connection")
            self._connection.close()
            try:
                self._shutdown_queue.get(timeout=10)
                self._shutdown_queue.join()
            except Exception as e:
                print(f"Exception while waiting for shutdown {e}")
        

    def connect(self):
        parameters = URLParameters(self._address)

        self._connection = SelectConnection(
            on_open_callback=self.on_open_connection,
            on_close_callback=self.on_connection_closed,
            on_open_error_callback=self.on_open_error,
            parameters=parameters,
        )
        self._connection.ioloop.start()

    def on_open_error(self, connection: SelectConnection, exception: Exception):
        print("Failed to open connection")
        self._connection.ioloop.stop()

    def wait_for_connection(self):
        try:
            self._connected_queue.get(timeout=10)
            self._connected_queue.join()
        except Exception as e:
            print(f"Exception occured while waiting for wait_for_connection {e}")

    def run(self):
        self._finished = False
        while not self._shutdown:
            self.connect()
        self._finished = True
        print("Stopped")


class RMQConsumer(RMQ):
    def __init__(
        self,
        queueName: str,
        exchangeName: str,
        routing_key: str = "",
        address: str = "",
        on_message: callable = None,
    ):
        super().__init__(
            queueName=queueName,
            exchangeName=exchangeName,
            routing_key=routing_key,
            address=address,
            on_message=on_message
        )
        self.start()
        self.wait_for_connection()
        self.queue_size_ = -1

    def on_queue_size(self, queue_notify : Queue, method_frame : frame.Method):
        self.queue_size_ = method_frame.method.message_count
        queue_notify.put(None)
        queue_notify.task_done()

    def on_queue_delete(self, event: threading.Event, method_frame: frame.Method):
        if method_frame.method.NAME == "Queue.DeleteOk":
            print("Deleted queue {}".format(self._queue))
        else:
            print("Failed to delete queue {}".format(self._queue))

    def getQueueSize(self, timeout : int = 10 ) -> int:
        self.queue_size_ = -1
        if not self._is_connected or self._channel is None:
            return self.queue_size_
        queue_notify = Queue()
        queue_size_event_callback = functools.partial(self.on_queue_size, queue_notify)
        self._channel.queue_declare(
            queue=self._queue,
            callback=queue_size_event_callback,
            durable=True)
        try:
            queue_notify.get(timeout=timeout)
            queue_notify.join()
        except Exception as e:
            print(f"Exception occured while waiting for queue_declare callback {e}")
        return self.queue_size_

    def deleteQueue(self, timeout: int = 10):
        # If we're not setup don't do anything
        if not self._is_connected or self._channel is None:
            return

        event = threading.Event()
        delete_event_callback = functools.partial(self.on_queue_delete, event)
        print("Deleting queue {}".format(self._queue))
        self._channel.queue_delete(queue=self._queue, callback=delete_event_callback)
        event.wait(timeout)
