import json
import logging

import sentry_sdk
from substrateinterface import Keypair

from aleph.chains.common import get_verification_buffer
from aleph.chains.register import register_verifier

LOGGER = logging.getLogger('chains.substrate')
CHAIN_NAME = 'DOT'

async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    
    try:
        signature = json.loads(message['signature'])
    except Exception as e:
        LOGGER.exception("Substrate signature deserialization error")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        return False
    
    try:
        if signature.get('curve', 'sr25519') != 'sr25519':
            LOGGER.warning('Unsupported curve %s' % signature.get('curve'))
    except Exception as e:
        LOGGER.exception("Substrate signature Key error")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        return False
    
    
    try:
        keypair = Keypair(ss58_address=message['sender'])
        verif = (await get_verification_buffer(message)).decode('utf-8')
        result = keypair.verify(verif, signature['data'])
    except Exception as e:
        LOGGER.exception("Substrate Signature verification error")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        result = False
        
    return result

register_verifier(CHAIN_NAME, verify_signature)

