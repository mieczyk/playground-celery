# Execute worker: celery -A task_events_monitoring.tasks worker --level=INFO -E
# REMEMBER: In order to make worker to send events the -E (--task-event) option
# must be used ('worker_send_task_events' setting is disabled by default).
# 'task_send_sent_event' setting (since version 2.2) = If enabled, a "task-sent" event will be
# sent for every task so tasks can be tracked before they’re consumed by a worker.

from celery import Celery

class CeleryEventsHandler:
    def __init__(self, celery_app):
        self._app = celery_app
        self._state = celery_app.events.State()

    def _print_event(self, event):
        print('[{}]'.format(event['type'].upper()))
        print('{}\n'.format(event))

    def _event_handler(handler):
        def wrapper(self, event):
            self._print_event(event)
            self._state.event(event)
            
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
