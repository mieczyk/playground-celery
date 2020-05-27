# Execute worker: celery -A task_events_monitoring.tasks worker --loglevel=INFO -E
# REMEMBER: In order to make worker to send events the -E (--task-event) option
# must be used ('worker_send_task_events' setting is disabled by default).
# 'task_send_sent_event' setting (since version 2.2) = If enabled, a "task-sent" event will be
# sent for every task so tasks can be tracked before they’re consumed by a worker.

from celery import Celery
from datetime import datetime as dt

class CeleryEventsHandler:
    def __init__(self, celery_app):
        self._app = celery_app
        self._state = celery_app.events.State()

    def _print_event(self, event):
        print('========== [{}] =========='.format(event['type'].upper()))
        print('{}\n'.format(event))

    def _print_task(self, task):
        print('UUID: {}'.format(task.uuid))
        print('Name: {}'.format(task.name))
        print('State: {}'.format(task.state))
        print('Received: {}'.format(task.received))
        print('Sent: {}'.format(task.sent))
        print('Started: {}'.format(task.started))
        print('Rejected: {}'.format(task.rejected))
        print('Succeeded: {}'.format(task.succeeded))
        print('Failed: {}'.format(task.failed))
        print('Retried: {}'.format(task.retried))
        print('Revoked: {}'.format(task.revoked))
        print('args (arguments): {}'.format(task.args))
        print('kwargs (keyword arguments): {}'.format(task.kwargs))
        print('ETA (Estimated Time of Arrival): {}'.format(task.eta))
        print('Expires: {}'.format(task.expires))
        print('Retries: {}'.format(task.retries))
        print('Worker: {}'.format(task.worker))
        print('Result: {}'.format(task.result))
        print('Exception: {}'.format(task.exception))
        print('Timestamp: {}'.format(task.timestamp))
        print('Runtime: {}'.format(task.runtime))
        print('Traceback: {}'.format(task.traceback))
        print('Exchange: {}'.format(task.exchange))
        print('Routing Key: {}'.format(task.routing_key))
        print('Clock: {}'.format(task.clock))
        print('Client: {}'.format(task.client))
        print('Root: {}'.format(task.root))
        print('Root ID: {}'.format(task.root_id))
        print('Parent: {}'.format(task.parent))
        print('Parent ID: {}'.format(task.parent_id))
        print('Children:')
        for child in task.children:
            print('\t{}\n'.format(str(child)))
        print('\n')

    def _event_handler(handler):
        def wrapper(self, event):
            self._print_event(event)
            
            self._state.event(event)
            task = self._state.tasks.get(event['uuid'])
            self._print_task(task)
            
            handler(self, event)
        return wrapper

    @_event_handler
    def _on_task_sent(self, event):
        pass

    @_event_handler
    def _on_task_received(self, event):
        pass

    @_event_handler
    def _on_task_started(self, event):
        pass

    @_event_handler
    def _on_task_succeeded(self, event):
        pass

    @_event_handler
    def _on_task_failed(self, event):
         pass

    @_event_handler
    def _on_task_rejected(self, event):
         pass

    @_event_handler
    def _on_task_revoked(self, event):
         pass

    @_event_handler
    def _on_task_retried(self, event):
        pass

    def start_listening(self):
        with self._app.connection() as connection:
            recv = self._app.events.Receiver(connection, handlers={
                'task-sent': self._on_task_sent,
                'task-received': self._on_task_received,
                'task-started': self._on_task_started,
                'task-succeeded': self._on_task_succeeded,
                'task-failed': self._on_task_failed,
                'task-rejected': self._on_task_rejected,
                'task-revoked': self._on_task_revoked,
                'task-retried': self._on_task_retried
            })
            recv.capture(limit=None, timeout=None, wakeup=True)

if __name__ == '__main__':
    # Use RabbitMQ with default credentials as a broker.
    app = Celery('task_events_monitoring.tasks', broker='pyamqp://guest@localhost')

    events_handler = CeleryEventsHandler(app)
    events_handler.start_listening()
