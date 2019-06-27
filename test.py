import base64
import hashlib, binascii

password = 'shikabet'
payment_id = '1'
msisdn=255716016240
amount=1000
digest = "%s%s%s%s" % (payment_id, password, msisdn, amount)
dk = hashlib.sha256(digest).digest()
hex_key = binascii.hexlify(dk).upper()
b64 = base64.b64encode(bytes(hex_key))

print b64 == 'QUIwMzY2MENGMERDMThCQ0UzQTk2RTY2MEFGQTMzNEE1QUZGQTY5NEYzOEQyMDE2MEQ2MzY3ODEwQzEwQ0U3NA=='
