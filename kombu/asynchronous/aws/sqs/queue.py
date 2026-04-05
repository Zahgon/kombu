"""Amazon SQS queue implementation."""

from __future__ import annotations

from vine import transform

from .message import AsyncMessage

_all__ = ['AsyncQueue']


def list_first(rs):
    """Get the first item in a list, or None if list empty."""
    pass


class AsyncQueue:
    """Async SQS Queue."""

    def __init__(self, connection=None, url=None, message_class=AsyncMessage):
        self.connection = connection
        self.url = url
        self.message_class = message_class
        self.visibility_timeout = None

    def _NA(self, *args, **kwargs):
        raise NotImplementedError()
    count_slow = dump = save_to_file = save_to_filename = save = \
        save_to_s3 = load_from_s3 = load_from_file = load_from_filename = \
        load = clear = _NA

    def get_attributes(self, attributes='All', callback=None):
        return self.connection.get_queue_attributes(
            self, attributes, callback,
        )

    def set_attribute(self, attribute, value, callback=None):
        pass

    def get_timeout(self, callback=None, _attr='VisibilityTimeout'):
        pass

    def _coerce_field_value(self, key, type, response):
        pass

    def set_timeout(self, visibility_timeout, callback=None):
        pass

    def _on_timeout_set(self, visibility_timeout):
        pass

    def add_permission(self, label, aws_account_id, action_name,
                       callback=None):
        pass

    def remove_permission(self, label, callback=None):
        pass

    def read(self, visibility_timeout=None, wait_time_seconds=None,
             callback=None):
        return self.get_messages(
            1, visibility_timeout,
            wait_time_seconds=wait_time_seconds,
            callback=transform(list_first, callback),
        )

    def write(self, message, delay_seconds=None, callback=None):
        return self.connection.send_message(
            self, message.get_body_encoded(), delay_seconds,
            callback=transform(self._on_message_sent, callback, message),
        )

    def write_batch(self, messages, callback=None):
        pass

    def _on_message_sent(self, orig_message, new_message):
        pass

    def get_messages(self, num_messages=1, visibility_timeout=None,
                     attributes=None, wait_time_seconds=None, callback=None):
        return self.connection.receive_message(
            self, number_messages=num_messages,
            visibility_timeout=visibility_timeout,
            attributes=attributes,
            wait_time_seconds=wait_time_seconds,
            callback=callback,
        )

    def delete_message(self, message, callback=None):
        return self.connection.delete_message(self, message, callback)

    def delete_message_batch(self, messages, callback=None):
        pass

    def change_message_visibility_batch(self, messages, callback=None):
        pass

    def delete(self, callback=None):
        return self.connection.delete_queue(self, callback=callback)

    def count(self, page_size=10, vtimeout=10, callback=None,
              _attr='ApproximateNumberOfMessages'):
        return self.get_attributes(
            _attr, callback=transform(
                self._coerce_field_value, callback, _attr, int,
            ),
        )
