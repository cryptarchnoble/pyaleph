

import json
from subprocess import Popen, PIPE

from pkg_resources import resource_filename, resource_string


def test_executor_error_1():
    process = Popen(['python',
                     resource_filename('aleph.vms.dockerized', 'tools/executor.py')],
                    stdout=PIPE, stderr=PIPE, stdin=PIPE)
    example_code = resource_string('aleph.vms.dockerized', 'tools/example.py')
    stdout, stderr = process.communicate(input=b"blahblah"*10)
    print(stdout.decode('utf-8'))
    print(stderr.decode('utf-8'))
    assert stderr == b''
    out_payload = json.loads(stdout.decode('utf-8'))
    assert out_payload['error'] == 'JSONDecodeError(\'Expecting value: line 1 column 1 (char 0)\')'
    assert out_payload['error_step'] == 'unhandled'
    assert out_payload['result'] == None


def test_executor_create():
    process = Popen(['python',
                     resource_filename('aleph.vms.dockerized', 'tools/executor.py')],
                    stdout=PIPE, stderr=PIPE, stdin=PIPE)
    example_code = resource_string('aleph.vms.dockerized', 'tools/example.py')
    payload = {
        'code': example_code.decode('utf-8'),
        'action': 'create',
        'message': {
            'sender': 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
        },
        'args': ['Test', 'TST', 24000000]
    }
    stdout, stderr = process.communicate(input=json.dumps(payload).encode('utf-8'))
    print(stdout.decode('utf-8'))
    print(stderr.decode('utf-8'))
    out_payload = json.loads(stdout.decode('utf-8'))
    assert out_payload['result'] is True
    assert out_payload['state']['owner'] == 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
    assert out_payload['state']['total_supply'] == 24000000 * (10**18)
    assert out_payload['state']['balances']['NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'] == 24000000 * (10**18)
    assert len(out_payload['state']['balances']) == 1
    assert stderr == b''

   
def test_executor_call_error():
    process = Popen(['python',
                     resource_filename('aleph.vms.dockerized', 'tools/executor.py')],
                    stdout=PIPE, stderr=PIPE, stdin=PIPE)
    example_code = resource_string('aleph.vms.dockerized', 'tools/example.py')
    payload = {
        'code': example_code.decode('utf-8'),
        'action': 'call',
        'function': 'transfer',
        'message': {
            'sender': 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
        },
        'state': {
            "owner": "NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W",
            "name": "Test",
            "symbol": "TST",
            "decimals": 18,
            "total_supply": 24000000000000000000000000,
            "balances": {"NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W": 24000000000000000000000000},
            "allowed": {}
        },        
        'args': ['blah', -10]
    }
    stdout, stderr = process.communicate(input=json.dumps(payload).encode('utf-8'))
    print(stdout.decode('utf-8'))
    print(stderr.decode('utf-8'))
    out_payload = json.loads(stdout.decode('utf-8'))
    assert 'error' in out_payload
    assert 'state' not in out_payload
    assert out_payload['result'] is not True
    assert out_payload['error'] == "AssertionError('Amount should be positive')"
    assert stderr == b''


def test_executor_call():
    process = Popen(['python',
                     resource_filename('aleph.vms.dockerized', 'tools/executor.py')],
                    stdout=PIPE, stderr=PIPE, stdin=PIPE)
    example_code = resource_string('aleph.vms.dockerized', 'tools/example.py')
    payload = {
        'code': example_code.decode('utf-8'),
        'action': 'call',
        'function': 'transfer',
        'message': {
            'sender': 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
        },
        'state': {
            "owner": "NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W",
            "name": "Test",
            "symbol": "TST",
            "decimals": 18,
            "total_supply": 24000000000000000000000000,
            "balances": {"NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W": 24000000000000000000000000},
            "allowed": {}
        },        
        'args': ['blah', 1000*(10**18)]
    }
    stdout, stderr = process.communicate(input=json.dumps(payload).encode('utf-8'))
    out_payload = json.loads(stdout.decode('utf-8'))
    assert out_payload['result'] is True
    assert out_payload['state']['owner'] == 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
    assert out_payload['state']['total_supply'] == 24000000 * (10**18)
    assert out_payload['state']['balances']['NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'] == (24000000 - 1000) * (10**18)
    assert out_payload['state']['balances']['blah'] == 1000 * (10**18)
    assert len(out_payload['state']['balances']) == 2
    assert stderr == b''
    
    
def test_executor_in_docker():
    import epicbox

    PROFILES = {
        'python': {
            'docker_image': 'python:3.8-alpine',
            'network_disabled': True,
            'user': '10000'
        }
    }

    epicbox.configure(
        profiles=PROFILES
    )
    files = [{
        'name': 'executor.py',
        'content': resource_string('aleph.vms.dockerized', 'tools/executor.py')}]
    limits = {'cputime': 2, 'memory': 256}
    example_code = resource_string('aleph.vms.dockerized', 'tools/example.py')
    
    payload = {
        'code': example_code.decode('utf-8'),
        'action': 'call',
        'function': 'transfer',
        'message': {
            'sender': 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
        },
        'state': {
            "owner": "NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W",
            "name": "Test",
            "symbol": "TST",
            "decimals": 18,
            "total_supply": 24000000000000000000000000,
            "balances": {"NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W": 24000000000000000000000000},
            "allowed": {}
        },        
        'args': ['blah', 1000*(10**18)]
    }
    
    output = epicbox.run('python', 'python3 executor.py', files=files, limits=limits,
                         stdin=json.dumps(payload).encode('utf-8'))
    
    out_payload = json.loads(output['stdout'].decode('utf-8'))
    assert out_payload['result'] is True
    assert out_payload['state']['owner'] == 'NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'
    assert out_payload['state']['total_supply'] == 24000000 * (10**18)
    assert out_payload['state']['balances']['NULSd6HgcNwprmEYbQ7pqLznGVU3EhW7Syv7W'] == (24000000 - 1000) * (10**18)
    assert out_payload['state']['balances']['blah'] == 1000 * (10**18)
    assert len(out_payload['state']['balances']) == 2
    assert not output.get('stderr')