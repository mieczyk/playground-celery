from celery import Celery

# The 'backend' option causes the results are kept in the RabbitMQ queue.
app = Celery('tasks', broker='pyamqp://guest@localhost', backend='rpc://')

@app.task
def add(x, y):
    return x + y

# celery -A tasks worker <- starts the celery worker that uses RabbitMQ.
# result = tasks.add(4, 5).delay() <- returns AsyncResult
# result.ready() <- is the task finished?
# result.get() <- get the task's results.
