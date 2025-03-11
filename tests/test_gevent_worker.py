import time
from unittest import mock

import pytest

from dramatiq import Worker
from dramatiq.brokers.stub import StubBroker
from dramatiq.gevent_worker import GeventWorker

from .common import worker, skip_without_gevent


def gevent_worker(*args, **kwargs):
    """A context manager that creates a GeventWorker and automatically
    stops it when the context manager exits.
    
    This is equivalent to the worker context manager in common.py but
    uses GeventWorker instead of Worker.
    """
    try:
        worker = GeventWorker(*args, **kwargs)
        worker.start()
        yield worker
    finally:
        worker.stop()


@skip_without_gevent
def test_gevent_worker_can_be_instantiated(stub_broker):
    # Given that I have a stub broker
    # When I create a GeventWorker with it
    worker = GeventWorker(stub_broker)
    
    # Then the worker should be a valid instance
    assert isinstance(worker, GeventWorker)
    
    # And it should have the expected attributes
    assert worker.broker == stub_broker
    assert worker.consumers == {}
    assert worker.worker_threads == 8  # default value


@skip_without_gevent
def test_gevent_worker_can_start_and_stop(stub_broker):
    # Given that I have a stub broker
    # When I start a worker
    worker = GeventWorker(stub_broker)
    worker.start()
    
    # Then join its queues to make sure it can process messages
    worker.join()
    
    # And stop the worker
    worker.stop()
    
    # It should exit cleanly
    assert True


@skip_without_gevent
def test_gevent_worker_is_compatible_with_worker_interface(stub_broker):
    # Given that I have a generic worker and a gevent worker
    regular_worker = Worker(stub_broker)
    gevent_worker = GeventWorker(stub_broker)
    
    # Both should implement the same public methods 
    for method_name in dir(regular_worker):
        if method_name.startswith('_'):
            continue
            
        assert hasattr(gevent_worker, method_name), f"GeventWorker is missing method: {method_name}"
        assert callable(getattr(gevent_worker, method_name)) == callable(getattr(regular_worker, method_name))


@skip_without_gevent
def test_gevent_workers_dont_register_queues_that_arent_whitelisted(stub_broker):
    # Given that I have a gevent worker object with a restricted set of queues
    with gevent_worker(stub_broker, queues={"a", "b"}) as worker:
        # When I try to register a consumer for a queue that hasn't been whitelisted
        stub_broker.declare_queue("c")
        stub_broker.declare_queue("c.DQ")

        # Then a consumer should not get spun up for that queue
        assert "c" not in worker.consumers
        assert "c.DQ" not in worker.consumers


@skip_without_gevent
def test_gevent_worker_uses_greenlets_instead_of_threads(stub_broker):
    # Given that I have a broker
    # When I create a GeventWorker with it
    with gevent_worker(stub_broker, worker_threads=2) as worker:
        # Then the worker's internal components should be greenlets
        from gevent import Greenlet
        
        # Consumers should be greenlets
        assert len(worker.consumers) > 0
        for consumer in worker.consumers.values():
            assert isinstance(consumer, Greenlet)
            
        # Workers should be greenlets
        assert len(worker.workers) == 2
        for worker_thread in worker.workers:
            assert isinstance(worker_thread, Greenlet)


@skip_without_gevent
def test_gevent_worker_pause_and_resume(stub_broker):
    # Given that I have a broker and a worker
    with gevent_worker(stub_broker) as worker:
        # When I pause the worker
        worker.pause()
        
        # Then its consumers should be paused
        for consumer in worker.consumers.values():
            assert consumer.paused is True
            
        # And its workers should be paused
        for worker_thread in worker.workers:
            assert worker_thread.paused is True
        
        # When I resume the worker
        worker.resume()
        
        # Then its consumers should no longer be paused
        for consumer in worker.consumers.values():
            assert consumer.paused is False
            
        # And its workers should no longer be paused
        for worker_thread in worker.workers:
            assert worker_thread.paused is False


@skip_without_gevent
def test_gevent_worker_join_method(stub_broker):
    # Given that I have a broker and a worker
    with gevent_worker(stub_broker) as worker:
        # When I call the join method
        # It should return without errors
        worker.join()


@skip_without_gevent
def test_gevent_worker_processes_messages(stub_broker):
    # Given that I have a broker and a worker
    with gevent_worker(stub_broker) as worker:
        # And I have an actor that records when it's called
        calls = []
        
        @stub_broker.actor
        def record():
            calls.append(1)
        
        # When I send that actor a message
        record.send()
        
        # And join on the queue
        stub_broker.join(record.queue_name)
        worker.join()
        
        # Then the message should have been processed
        assert sum(calls) == 1