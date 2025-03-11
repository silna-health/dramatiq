import sys
import pytest
from dramatiq.cli import import_worker_class

from .common import skip_without_gevent


def test_import_worker_class_with_existing_class():
    # Given a valid worker class path
    worker_class_path = "dramatiq.Worker"
    
    # When I import it
    worker_class = import_worker_class(worker_class_path)
    
    # Then I should get back the Worker class
    from dramatiq import Worker
    assert worker_class is Worker


@skip_without_gevent
def test_import_worker_class_with_gevent_worker():
    # Given a valid gevent worker class path
    worker_class_path = "dramatiq.gevent_worker.GeventWorker"
    
    # When I import it
    worker_class = import_worker_class(worker_class_path)
    
    # Then I should get back the GeventWorker class
    from dramatiq.gevent_worker import GeventWorker
    assert worker_class is GeventWorker


def test_import_worker_class_with_invalid_module():
    # Given an invalid module path
    worker_class_path = "nonexistent_module.Worker"
    
    # When I try to import it, I should get an ImportError
    with pytest.raises(ImportError):
        import_worker_class(worker_class_path)


def test_import_worker_class_with_invalid_class():
    # Given a valid module but invalid class
    worker_class_path = "dramatiq.NonexistentClass"
    
    # When I try to import it, I should get an ImportError
    with pytest.raises(ImportError):
        import_worker_class(worker_class_path)


def test_import_worker_class_with_invalid_format():
    # Given an incorrectly formatted worker class path
    worker_class_path = "dramatiq"
    
    # When I try to import it, I should get an ImportError
    with pytest.raises(ImportError):
        import_worker_class(worker_class_path)


def test_import_worker_class_with_non_worker_class():
    # Given a valid class that's not a worker class
    worker_class_path = "dramatiq.Message"
    
    # When I try to import it, I should get an ImportError
    with pytest.raises(ImportError):
        import_worker_class(worker_class_path)