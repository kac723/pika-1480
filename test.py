from RMQ import RMQConsumer
import time


cons = list()
# Create consumers
for k in range(4):
    cons.append(
        RMQConsumer(
            f"test{k}", address="amqp://guest:guest@localhost", exchangeName="test"
        )
    )
time.sleep(1)
# Run getQueueSize in a loop and meassure time
for j in range(36000):
    timeout = False
    for c in cons:
        start = time.time()
        size = c.getQueueSize()
        dur = time.time() - start
        print(f"getQueueSize took {dur} for queue {c.queue_}")
        if dur > 10:
            print(
                "Test failed. getQueueSize took more than 10 seconds. This will cause the script to block as we cannot close consumers"
            )
            timeout = True
    if timeout:
        break
time.sleep(1)
# Stop consumers and wait for them to shutdown cleanly
for c in cons:
    c.shutdown()
for c in cons:
    while not c.finished_:
        time.sleep(0.1)
print(f"finished")
