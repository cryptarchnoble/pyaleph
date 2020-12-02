import base64
import hashlib
import json
import logging

import ecdsa
import sentry_sdk
from cosmospy._wallet import pubkey_to_address

from aleph.chains.common import get_verification_buffer
from aleph.chains.register import register_verifier

LOGGER = logging.getLogger('chains.cosmos')
CHAIN_NAME = 'CSDK'


async def get_signable_message(message):
    signable = (await get_verification_buffer(message)).decode('utf-8')
    content_message = {
        "type": "signutil/MsgSignText",
        "value": {
            "message": signable,
            "signer": message['sender'],
        },
    }
    
    return {
        "chain_id": "signed-message-v1",
        "account_number": str(0),
        "fee": {
            "amount": [],
            "gas": str(0),
        },
        "memo": "",
        "sequence": str(0),
        "msgs": [content_message,],
    }
    
    
async def get_verification_string(message):
    value = await get_signable_message(message)
    print(value)
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


async def get_hrp(address):
    hrp, rest = address.split("1", 1)
    return hrp


async def verify_signature(message):
    """ Verifies a signature of a message, return True if verified, false if not
    """
    
    try:
        signature = json.loads(message['signature'])
    except Exception as e:
        LOGGER.exception("Cosmos signature deserialization error")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        return False
    
    try:
        if signature.get('pub_key').get('type') != 'tendermint/PubKeySecp256k1':
            LOGGER.warning('Unsupported curve %s' % signature.get('pub_key').get('type'))
    except Exception as e:
        LOGGER.exception("Cosmos signature Key error")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        return False
    
    
    try:
        pub_key = base64.b64decode(signature.get('pub_key').get('value'))
        hrp = await get_hrp(message['sender'])
    except Exception as e:
        LOGGER.exception("Cosmos key verification error")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        result = False
        
    try:
        sig_compact = base64.b64decode(signature.get('signature'))
    except Exception as e:
        LOGGER.exception("Cosmos signature deserialization error")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        result = False
    
    
    try:
        address = pubkey_to_address(pub_key, hrp=hrp)
        if address != message['sender']:
            LOGGER.warning('Signature for bad address %s instead of %s' % (address, message['sender']))
            return False
        
        verif = await get_verification_string(message)
        vk = ecdsa.VerifyingKey.from_string(pub_key, curve=ecdsa.SECP256k1)
        verified = vk.verify(sig_compact, verif.encode("utf-8"), hashfunc=hashlib.sha256)
        return verified
    
    except Exception as e:
        LOGGER.exception("Substrate Signature verification error")
        sentry_sdk.capture_exception(e)
        sentry_sdk.flush()
        raise
        result = False
    
    return result

register_verifier(CHAIN_NAME, verify_signature)

