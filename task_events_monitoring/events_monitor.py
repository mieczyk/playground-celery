# Execute worker: celery -A prefetch_limit_worker_setting worker --level=INFO -E
# REMEMBER: In order to make worker to send events the -E (--task-event) option
# must be used ('worker_send_task_events' setting is disabled by default).
# 'task_send_sent_event' setting (since version 2.2) = If enabled, a "task-sent" event will be
# sent for every task so tasks can be tracked before they’re consumed by a worker.

import time
from celery import Celery

# Use RabbitMQ with default credentials as a broker.
app = Celery('task_events', broker='pyamqp://guest@localhost')
app.conf.task_send_sent_event = True

# Define a long running task (1 minute)
@app.task
def long_running_task():
    start = time.perf_counter()
    time.sleep(60)
    end = time.perf_counter()

    return '[long_running_task] Execution time: {0:.4f}s'.format(end-start)

# Define a very fast task (0.05 second)
@app.task
def fast_task():
    start = time.perf_counter()
    time.sleep(0.05)
    end = time.perf_counter()

    return '[fask_task] Execution time: {0:.4f}s'.format(end-start)

# Monitor Celery activities
def celery_monitor():
    # app.events.State is a convenient in-memory representation of tasks 
    # and workers in the cluster that’s updated as events come in.
    state = app.events.State()

    def on_task_sent(event):
        state.event(event)
        task = state.tasks.get(event['uuid'])
        print(type(event))
        print(event)
        print(task.queue)

    def on_task_received(event):
        pass

    def on_task_started(event):
        pass

    def on_task_succeeded(event):
        state.event(event)
        # task name is sent only with -received event, and state
        # will keep track of this for us.
        task = state.tasks.get(event['uuid'])
        print('TASK SUCCEEDED: %s[%s] %s' % (
            task.name, task.uuid, task.info(),))
    
    def on_task_failed(event):
        pass

    def on_task_rejected(event):
        pass

    def on_task_retried(event):
        pass

    def on_task_event(event):
        state.event(event)
        print(event)
        #task = state.tasks.get(event['uuid'])
        #print(task.info())

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
                'task-sent': on_task_sent,
                'task-succeeded': on_task_succeeded,
                '*': state.event, # Will catch ALL events, including worker-heartbeat
        })
        recv.capture(limit=None, timeout=None, wakeup=True)

if __name__ == '__main__':
    celery_monitor()
