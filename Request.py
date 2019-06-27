from Db import Db
import requests


class Process:

    def __init__(self, logger, data, message):
        self.logger = logger
        self.logger.info('INIT %r' % data)
        self.data = data
        self.message = message
        self.db = self.get_db()
        self.logger.info('INIT DONE')

    def get_db(self):
        return Db(self.logger)

    def call_mpesa_online(self, msisdn, amount):
        url = "http://localhost/monline/src/php/"
        payload = {
                'phone': msisdn,
                'amount': amount,
                'request': "mpesa",
                'reference_id': 'Your SHIKABET Account',
                'callback': "http://",
                'paybill': "101010"
            }
        try:
            output = requests.post(url, data=payload, timeout=30)
            self.logger.info("Found result from sdp call: (%s) "
             % (output.text, ))
            return "Success"
        except requests.exceptions.RequestException as e:
            self.logger.error("Exception invoke mpesa online: %r %r"
             % (payload, e))
            return "Failed"

    def call_mpesa_online_demo(self, msisdn, amount, reference):
        url = "http://localhost/monline/src/php/"
        payload = {
                'phone': msisdn,
                'amount': amount,
                'request': "mpesa",
                'reference_id': reference,
                'callback': "http://",
                'paybill': "880088"
            }
        try:
            output = requests.post(url, data=payload, timeout=30)
            self.logger.info("Found result from sdp call: (%s) "
             % (output.text, ))
            return "Success"
        except requests.exceptions.RequestException as e:
            self.logger.error("Exception invoke mpesa online: %r %r"
             % (payload, e))
            return "Failed"

    def  process_request(self):
        self.logger.info('Process Q called: %r' % self.data)
        result = None
        queue_type = self.data.get("queueType", "").lower()
        exchange = self.message.delivery_info.get('exchange') if self.message else self.data.get('exchange')

        self.logger.info('Process extracted exhange: %r' % exchange)
        if queue_type  == "withdrawal":
            self.logger.info('::processing withdrawal::')
            result = self.db.retry_create_withdrawal(self.data)
            self.logger.info('withdrawal recreate result::: %r' % result)

        elif queue_type == "mpesa_withdrawal":
            self.logger.info('::processing withdraw messages::')
            result = self.db.request_withdrawal(self.data)
            self.logger.info('mpesa withdraw message process  result : %r' % result)

        elif exchange == 'SHIKABET_WINNER_MESSAGES_QUEUE':
            self.logger.info('::processing win messages::')
            result = self.db.send_win_message(self.data)
            self.logger.info('win_message process  result : %r' % result)

        elif exchange == "C2B_EXCHANGE":
            self.logger.info('::processing deposit messages::')
            result = self.db.deposit(self.data)
            self.logger.info('mpesa deposit message process  result : %r' % result)
        elif (exchange == "SHIKABET_SENDSMS_EX" or exchange == "SHIKABET_SENDSMS"):
            self.logger.info('::processing sendsms api messages::')
            result = self.db.terminate_sendsms_api_message(self.data)
            self.logger.info('send message invoke result : %r' % result)
        elif exchange == "WITHDRAWAL_STATUS":
            self.logger.info('::processing reverse of withdrawals')
            result = self.db.reverse_withdrawals(self.data)
            self.logger.info('reverse withdrawal got result : %r' % result)
        else:
            self.logger.info('Oops unrecognized message ... pushing it back to queue:')
            result = None

        self.db.close()

        if not self.message:
            return result

        if not result:
            #self.message.reject(requeue=True)
            self.logger.info('messsage rejected and requeued')
        else:
            #self.message.ack()
            self.logger.info('messsage acked')

        return result
