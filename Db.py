from sqlalchemy import BigInteger, Column, Date, DateTime, Enum, Float,\
 ForeignKey, Integer, Numeric, SmallInteger, String, Text, text, Index
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.engine import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
import hashlib
import uuid
from random import randint
from sqlalchemy.exc import IntegrityError
Base = declarative_base()
metadata = Base.metadata

from utils import LocalConfigParser
import requests
import json
import time
from datetime import datetime, timedelta
from sqlalchemy.sql import text as sql_text
import re

class Bet(Base):
    __tablename__ = 'bet'

    bet_id = Column(Integer, primary_key=True)
    profile_id = Column(ForeignKey(u'profile.profile_id'), nullable=False, index=True)
    bet_message = Column(String(200), nullable=False)
    total_odd = Column(Float(asdecimal=True), nullable=False)
    bet_amount = Column(Numeric(10, 2), nullable=False)
    possible_win = Column(Numeric(10, 2), nullable=False)
    status = Column(SmallInteger, nullable=False)
    win = Column(Integer, nullable=False, server_default=text("'0'"))
    created_by = Column(String(70), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False)

    profile = relationship(u'Profile')


class BetSlip(Base):
    __tablename__ = 'bet_slip'

    bet_slip_id = Column(Integer, primary_key=True)
    parent_match_id = Column(ForeignKey(u'match.parent_match_id'), nullable=False, index=True)
    bet_id = Column(ForeignKey(u'bet.bet_id'), nullable=False, index=True)
    bet_pick = Column(String(20), nullable=False)
    total_games = Column(Integer, nullable=False)
    odd_value = Column(Numeric(10, 2), nullable=False)
    win = Column(Integer, nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    status = Column(Integer, nullable=False)
    sub_type_id = Column(Integer, nullable=False)

    bet = relationship(u'Bet')
    parent_match = relationship(u'Match')


class Competition(Base):
    __tablename__ = 'competition'

    competition_id = Column(Integer, primary_key=True)
    competition_name = Column(String(120), nullable=False)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    status = Column(SmallInteger, nullable=False)
    sport_id = Column(ForeignKey(u'sport.sport_id'), nullable=False, index=True)
    created_by = Column(String(70), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    priority = Column(Integer, server_default=text("'10'"))

    sport = relationship(u'Sport')


class EventOdd(Base):
    __tablename__ = 'event_odd'

    event_odd_id = Column(Integer, primary_key=True)
    parent_match_id = Column(ForeignKey(u'match.parent_match_id'), index=True)
    sub_type_id = Column(ForeignKey(u'odd_type.sub_type_id'), index=True)
    max_bet = Column(Numeric(10, 0), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    odd_key = Column(String(5), nullable=False)
    odd_value = Column(Numeric(10, 2))
    odd_alias = Column(String(20))

    parent_match = relationship(u'Match')
    sub_type = relationship(u'OddType')


class GameRequest(Base):
    __tablename__ = 'game_request'

    request_id = Column(Integer, primary_key=True)
    match_id = Column(Integer, nullable=False)
    profile_id = Column(Integer, nullable=False)
    offset = Column(Integer, nullable=False)
    created = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class Inbox(Base):
    __tablename__ = 'inbox'

    inbox_id = Column(Integer, primary_key=True)
    network = Column(String(50))
    shortcode = Column(Integer)
    msisdn = Column(String(20))
    message = Column(String(300))
    linkid = Column(String(100))
    created = Column(DateTime)
    modified = Column(DateTime, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    created_by = Column(String(45))


class Match(Base):
    __tablename__ = 'match'

    match_id = Column(Integer, primary_key=True)
    parent_match_id = Column(Integer, nullable=False, unique=True)
    home_team = Column(String(50), nullable=False)
    away_team = Column(String(50), nullable=False)
    start_time = Column(DateTime, nullable=False)
    game_id = Column(String(6), nullable=False)
    competition_id = Column(ForeignKey(u'competition.competition_id'), nullable=False, index=True)
    status = Column(Integer, nullable=False)
    bet_closure = Column(DateTime, nullable=False)
    created_by = Column(String(60), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    result = Column(String(45))
    completed = Column(Integer, server_default=text("'0'"))
    priority = Column(Integer, server_default=text("'50'"))

    competition = relationship(u'Competition')


class MpesaRate(Base):
    __tablename__ = 'mpesa_rate'

    id = Column(BigInteger, primary_key=True)
    min_amount = Column(Float, nullable=False)
    max_amount = Column(Float, nullable=False)
    charge = Column(Float, nullable=False)
    created = Column(DateTime)
    updated = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))


class MpesaTransaction(Base):
    __tablename__ = 'mpesa_transaction'
    __table_args__ = (
        Index('mpesa_code_msisdn', 'mpesa_code', 'msisdn', unique=True),
    )

    mpesa_transaction_id = Column(BigInteger, primary_key=True)
    msisdn = Column(BigInteger, nullable=False)
    transaction_time = Column(DateTime, nullable=False)
    message = Column(String(300), nullable=False)
    mpesa_customer_id = Column(String(50), nullable=False)
    account_no = Column(String(100), nullable=False)
    mpesa_code = Column(String(100), nullable=False, unique=True)
    mpesa_amt = Column(Numeric(53, 2), nullable=False)
    mpesa_sender = Column(String(100), nullable=False)
    business_number = Column(Integer, nullable=False)
    enc_params = Column(String(250))
    promo_code = Column(String(50))
    paid_by = Column(String(50), nullable=True)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))


class OddType(Base):
    __tablename__ = 'odd_type'

    bet_type_id = Column(Integer, primary_key=True)
    name = Column(String(70), nullable=False)
    created_by = Column(String(70), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    sub_type_id = Column(Integer, nullable=False, index=True)


class Outbox(Base):
    __tablename__ = 'outbox'

    outbox_id = Column(BigInteger, primary_key=True)
    shortcode = Column(Integer)
    network = Column(String(50))
    profile_id = Column(ForeignKey(u'profile.profile_id'), index=True)
    linkid = Column(String(100))
    sdp_id = Column(String(100))
    date_created = Column(DateTime, index=True)
    date_sent = Column(DateTime)
    retry_status = Column(Integer, server_default=text("'0'"))
    modified = Column(DateTime,
         server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    text = Column(Text)
    msisdn = Column(String(25))

    profile = relationship(u'Profile')


class Outcome(Base):
    __tablename__ = 'outcome'

    match_result_id = Column(Integer, primary_key=True)
    sub_type_id = Column(Integer, nullable=False, index=True)
    parent_match_id = Column(ForeignKey(u'match.parent_match_id'), nullable=False, index=True)
    created_by = Column(String(70), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    status = Column(Integer, nullable=False, server_default=text("'0'"))
    winning_outcome = Column(String(10), nullable=False)

    parent_match = relationship(u'Match')


class Profile(Base):
    __tablename__ = 'profile'

    profile_id = Column(BigInteger, primary_key=True)
    msisdn = Column(String(45), unique=True)
    created = Column(DateTime)
    status = Column(SmallInteger)
    modified = Column(DateTime)
    created_by = Column(String(45))
    network = Column(String(50))


class ProfileSetting(Base):
    __tablename__ = 'profile_settings'

    profile_setting_id = Column(Integer, primary_key=True)
    profile_id = Column(Integer, nullable=False)
    balance = Column(Numeric(10, 2), nullable=False)
    status = Column(Integer, nullable=False)
    verification_code = Column(Integer)
    name = Column(String(255))
    reference_id = Column(String(20))
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    password = Column(Text, nullable=False)
    max_stake = Column(Numeric(10, 2))


class ProfileBalance(Base):
    __tablename__ = 'profile_balance'

    profile_balance_id = Column(Integer, primary_key=True)
    profile_id = Column(ForeignKey(u'profile.profile_id'), nullable=False, unique=True)
    balance = Column(Numeric(10, 2), nullable=False)
    bonus_balance = Column(Numeric(10, 2), nullable=True)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))

    profile = relationship(u'Profile')


class ProfileBonu(Base):
    __tablename__ = 'profile_bonus'

    profile_bonus_id = Column(Integer, primary_key=True)
    profile_id = Column(ForeignKey(u'profile.profile_id'), nullable=False, index=True)
    referred_msisdn = Column(String(25), nullable=False)
    bonus_amount = Column(Numeric(10, 2), nullable=False, server_default=text("'0.00'"))
    status = Column(Enum(u'NEW', u'CLAIMED', u'EXPIRED', u'CANCELLED'), nullable=False, server_default=text("'NEW'"))
    bet_on_status = Column(SmallInteger, nullable=True)
    expiry_date = Column(DateTime, nullable=False)
    date_created = Column(DateTime, nullable=False)
    updated = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP"))
    created_by = Column(String(55), nullable=True)

    profile = relationship(u'Profile')


class Sport(Base):
    __tablename__ = 'sport'

    sport_id = Column(Integer, primary_key=True)
    sport_name = Column(String(50), nullable=False)
    created_by = Column(String(70), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))


class Transaction(Base):
    __tablename__ = 'transaction'

    id = Column(Integer, primary_key=True)
    profile_id = Column(ForeignKey(u'profile.profile_id'), nullable=False, index=True)
    account = Column(String(50), nullable=False)
    iscredit = Column(SmallInteger, nullable=False)
    reference = Column(String(50), nullable=False)
    amount = Column(Numeric(10, 0), nullable=False)
    running_balance = Column(Numeric(10, 0), nullable=False)
    created_by = Column(String(60), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    profile = relationship(u'Profile')


class Winner(Base):
    __tablename__ = 'winner'

    winner_id = Column(Integer, primary_key=True)
    bet_id = Column(ForeignKey(u'bet.bet_id'), nullable=False, index=True)
    bet_amount = Column(Numeric(10, 0), nullable=False)
    win_amount = Column(Numeric(10, 0), nullable=False)
    profile_id = Column(ForeignKey(u'profile.profile_id'), nullable=False, index=True)
    credit_status = Column(SmallInteger, nullable=False, server_default=text("'0'"))
    created_by = Column(String(70), nullable=False)
    created = Column(DateTime, nullable=False)
    modified = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

    bet = relationship(u'Bet')
    profile = relationship(u'Profile')


class Withdrawal(Base):
    __tablename__ = 'withdrawal'

    withdrawal_id = Column(Integer, primary_key=True)
    inbox_id = Column(ForeignKey(u'inbox.inbox_id'), nullable=False, index=True)
    msisdn = Column(String(25), nullable=False, index=True)
    raw_text = Column(String(200), nullable=False)
    amount = Column(Numeric(64, 2), nullable=False)
    reference = Column(String(70), nullable=False)
    created = Column(DateTime, nullable=False, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))
    created_by = Column(String(45), nullable=False)
    status = Column(Enum(u'PROCESSING',u'QUEUED',u'SUCCESS',u'FAILED',u'TRX_SUCCESS',u'CANCELLED', u'SENT'), nullable=False, server_default=text("'PROCESSING'"))
    provider_reference = Column(String(250))
    number_of_sends = Column(Integer, nullable=False, server_default=text("'0'"))
    charge = Column(Float, nullable=False, server_default=text("'30'"))
    max_withdraw = Column(Float, nullable=False, server_default=text("'10000'"))

    inbox = relationship(u'Inbox')


class Db():

    APP_NAME = 'Shikabet_Q_Processor'

    def __del__(self):
        self.close()

    def __init__(self, logger, port=3306):

        self.db_configs = LocalConfigParser.parse_configs("DB")
        self.sdp_configs = LocalConfigParser.parse_configs("SDP")
        self.withdraw_configs = LocalConfigParser.parse_configs("WITHDRAWAL")
        self.bulk_configs = LocalConfigParser.parse_configs("BULK")
        self.shikabet_configs = LocalConfigParser.parse_configs("SHIKABET")
        self.host = self.db_configs['host']
        self.db = self.db_configs['db_name']
        self.username = self.db_configs['username']
        self.password = self.db_configs['password']
        self.port = self.db_configs['port'] or port

        self.db_uri = self.get_connection_uri()
        self.engine = self.get_engine()
        self.db_session = self.get_db_session()
        self.logger = logger
        self.profile_id = None

    def get_connection_uri(self):
        return "mysql+pymysql://{username}:{password}@{host}/{db}".format(
                username=self.username,
                password=self.password,
                host=self.host,
                db=self.db
            )

    def get_engine(self):
        return create_engine(self.db_uri, poolclass=NullPool)

    def get_db_session(self):
        return scoped_session(
                sessionmaker(autocommit=False, autoflush=False,
                     bind=self.engine)
            )

    def get_session(self):
        return self.db_session

    def straight_withdrawal(self, message, amount, charge):
        withdraw_dict = {
            "msisdn":message.get('msisdn'), 
            "inbox_id":None,
            "created":datetime.now(),
            "raw_text":message.get("message", ''),
            "amount":amount,
            "reference":'',
            "charge":charge,
            "status":'TRX_SUCCESS',
            "created_by":self.APP_NAME, 
            "network":self.get_network(message.get('msisdn'))
        }
        amount = amount + charge

        connection = self.engine.connect()
        trans = connection.begin()

        try:
            result_proxy= connection.execute(
                Withdrawal.__table__.insert(),withdraw_dict)
            withdrawal_id = result_proxy.inserted_primary_key

            trx_credit ={
                "profile_id":self.profile_id,
                "account":"%s_%s" % (self.profile_id, 'VIRTUAL'),
                "iscredit":0,
                "reference":str(withdrawal_id[0]) \
                    + "_" + message.get("msisdn"),
                "amount":amount,
                "created_by":self.APP_NAME,"created":datetime.now(),
                "modified":datetime.now()
            }

            connection.execute(Transaction.__table__.insert(), trx_credit)
           
            updateQL = "update profile_balance set balance = balance - %0.2f"\
                "  where profile_id = :pf" % (amount, )
            connection.execute(sql_text(updateQL), {'pf':self.profile_id})
            trans.commit()

            withdraw_dict['withdrawal_id'] = withdrawal_id[0]
            withdraw_dict['request_amount'] = amount
            withdraw_dict['amount'] = amount
            withdraw_dict['charge'] = charge

            self.logger.info("request saved success : %r " 
                % (withdraw_dict['withdrawal_id']))
            return withdraw_dict

        except IntegrityError, e:
            self.logger.error("Integrity Error skipping trx save ...%r" % e)
            trans.rollback()
            return False
        except Exception, e:
            self.logger.error("Problem creating transaction message ...%r" % e)
            trans.rollback()
            return False
        else:
            self.close()



    def close(self):
        try:
            self.db_session.remove()
            self.engine.dispose()
        except:
            pass

    def send_win_message(self, json_data):
        self.logger.info("Calling post_straight_to_sdp ..")
        url = self.bulk_configs["url"]

        #json_data = json_data.get("queue.QMessage", {})
        bet_id = json_data.get('bet_id', None)
        if bet_id:
            q = "select bs.bet_id, bet.possible_win, bet.profile_id from ticket_settlement bs inner join bet "\
                " on bs.bet_id=bet.bet_id  where bet.bet_id = :bet_id and bet.status=5"
            br = self.engine.execute(sql_text(q), {'bet_id':bet_id}).fetchone()
            if br:
                # this nigga did bet and paid via
                self.profile_id = br[2]
                str_w_d = self.straight_withdrawal(json_data, br[1], 0) 
                if str_w_d:
                    self.request_withdrawal(str_w_d)

        #payload = {
        #    "msisdn": json_data.get('msisdn'),
        #    "message": json_data.get('message') or json_data.get('text'),
        #    "access_code": self.bulk_configs["access_code"],
        #    "linkid": json_data.get("refNo"),
        #    "short_code": self.bulk_configs["bulk_code"],
        #    "correlator": outbox_id,
        #    "service_id": self.bulk_configs["service_id"]
        #}
        payload = {
                "PhoneNumber":json_data.get('msisdn'),
                "ticketNumber":json_data.get('refNo'),
                "network":json_data.get('network',""),
                "language":"sw",
                "Text":json_data.get('message') or json_data.get('text'),
                }


        self.logger.info("Calling SDP URL: (%s, %r) " % (url, payload))
        output = None

        try:
            output = requests.get(url, params=payload, timeout=30)
            self.logger.info("Found result from sdp call: (%s) "
             % (output.text, ))
        except requests.exceptions.RequestException as e:    # This is the correct syntax
            self.logger.error("Exception attempting to send MO message : %r "
             % payload)
            output = True

        return output

    def terminate_sendsms_api_message(self, json_data):
        self.logger.info("Received sendsmsapi message for terminationation %r configs %r"
         % (json_data, self.sdp_configs))
        if json_data.get("message_type") == 'MO':
            url = self.sdp_configs["url"]
        else:
            url = self.bulk_configs["url"]

        prepared_configs = self.get_configs_to_send_sms(json_data)
        self.logger.info("Prepare configs for sendsms %r" % prepared_configs)
        payload = {
            "msisdn": json_data.get('msisdn'),
            "phone": json_data.get('msisdn'),
            "message": json_data.get('message'),
            "access_code": prepared_configs.get('short_code')
             or self.bulk_configs["access_code"],
            "linkid": json_data.get('link_id'),
            "short_code": prepared_configs.get('short_code')
             or self.bulk_configs["bulk_code"],
            "correlator": json_data.get("correlator"),
            "service_id": prepared_configs.get("sdpid")
             or self.bulk_configs["service_id"],
             "serviceId": prepared_configs.get("sdpid")
        }
        sms_payload = {
           "PhoneNumber":payload.get('msisdn'),
            "ticketNumber":"",
            "network":json_data.get('network',""),                      
	    "language":"sw",
            "Text":payload.get('message')
	}

        self.logger.info("Prepared data for sendsms %r" % payload)
        outbox_id = self.outbox_message(payload)
        self.logger.info("After to insert outbox_message : %r " % outbox_id)

        if not outbox_id:
            self.logger.error("Failed to create outbox message : (%r) "
              % payload)
            return True
        self.logger.info("outbox not null : %s" % outbox_id)
        #payload['correlator'] = outbox_id

        self.logger.info("Calling SDP URL FOR Sendsms api terminate: (%s, %r) "
         % (url, payload))
        output = None

        try:
            output = requests.get(url, params=sms_payload, timeout=30)
            self.logger.info("Found result from sdp call: (%s) "
             % (output.text, ))
        except requests.exceptions.RequestException as e:
            self.logger.error("Exception attempting to send BULK sendsmsapi \
message : %r data %r " % e, payload)
            output = True

        return output

    def get_configs_to_send_sms(self, payload):
        if payload.get("access_code") == "1000" and payload.get("message_type") == "MO":
            sdpid = self.sdp_configs['service_id_mo']
            short_code = self.sdp_configs['code_shikabet_mo']
        elif payload.get("access_code") == "1000" and payload.get("message_type") == "BULK":
            sdpid = self.sdp_configs['service_id_shikabet_mo']
            short_code = self.sdp_configs['access_code_shikabet_mo']
        elif payload.get("access_code") == "SHIKABET" and payload.get("message_type") == "BULK":
            sdpid = self.sdp_configs['service_id_shikabet']
            short_code = self.sdp_configs['access_code_shikabet']
        else:
            sdpid = self.sdp_configs['service_id_shikabet_mo']
            short_code = self.sdp_configs['access_code_shikabet_mo']

        return {"sdpid": sdpid, "short_code": short_code}

    def send_bonus_redeem_ack(self, bonus_profile_id, status, referred_msisdn, redeemed=True):
        self.logger.info("Calling send mpesa deposit message .. Bonus")
        url = self.bulk_configs["url"]

        bonus_data_Q = """select p.msisdn, pb.balance, pb.bonus_balance from profile p
        inner join profile_balance pb using(profile_id) where
        profile_id = :pf"""
        self.logger.info("Bonus redeem Q %r" % bonus_data_Q);

        bonus_data_result = self.engine.execute(sql_text(bonus_data_Q),
        {'pf':bonus_profile_id}).fetchone()

        if not bonus_data_result or not bonus_data_result[0]:
            self.logger.info("MIssing bonus profile will ignore message ..")
            return

        if redeemed:
            message = 'Your bonus balance has been updated. Your new bonus balance is %0.2f. Send ACCEPT#NUMBER to refer more people to SHIKABET' % bonus_data_result[2]
        elif status == 'NEW':
            message = 'Your friend phone number %s topped up. But we could not award bonus. Minimum topup requied to claim bonus is Shs. 50. Please notify them to deposit Shs.50 and above to claim your bonus.' % (referred_msisdn)
        else:
             message = 'Your friend phone number %s topped up. But we could not award bonus because your bonus redeem time of 48hrs had expired. Your current bonus balance is Tshs. %0.2f Send ACCEPT#NUMBER to refer more people to SHIKABET' % (referred_msisdn, bonus_data_result[2])
        outbox_id = self.outbox_message({"message":message,
            'msisdn':bonus_data_result[0], "linkid":'', 'network':'NONE'})
        if not outbox_id:
            self.logger.info("MIssing outbox for bonus message  will ignore message ..")
            return

        payload = {
            "msisdn": bonus_data_result[0],
            "message": message,
            "access_code": self.bulk_configs["access_code"],
            "linkid": "%s_%s" % (bonus_profile_id, outbox_id),
            "short_code": self.bulk_configs["bulk_code"],
            "correlator": "%s_%s" % (bonus_profile_id, outbox_id),
            "service_id": self.bulk_configs["service_id"]
        }
        try:
            output = requests.post(url, data=payload, timeout=30)
            self.logger.info("Found result from SDP bulk call: (%s) "
             % (output.text, ))
        except requests.exceptions.RequestException as e:    # This is the correct syntax
            self.logger.error("Exception attempting to send BULK MPESA- BONUS \
message : %r: %r " % (payload, e))
            output = None
        return

    def send_mpesa_deposit_ack(self, json_data):
        self.logger.info("Calling send mpesa deposit message ..")
        url = self.bulk_configs["url"]

        payload = {
            "msisdn": json_data.get('msisdn'),
            "message": json_data.get('message') or json_data.get('text'),
            "access_code": self.bulk_configs["access_code"],
            "linkid": json_data.get("reference_id"),
            "short_code": self.bulk_configs["bulk_code"],
            "correlator": json_data.get("reference_id"),
            "reference_id": json_data.get("reference_id"),
            "service_id": self.bulk_configs["service_id"]
        }

        outbox_id = self.outbox_message(payload)
        self.logger.info("Hitting outbox_message : %r " % outbox_id)

        if not outbox_id:
            self.logger.error("Failed to create outbox message : (%r) "
              % payload)
            return False
        self.logger.info("outbox not null : %s" % outbox_id)
        payload['correlator'] = outbox_id

        output = None

        try:
            sms_payload = {
                "PhoneNumber":payload.get('msisdn'),
		"ticketNumber":payload.get('reference_id'),
		"network":json_data.get('network',""),
		"language":"sw",
		"Text":payload.get('message'),
		}
            self.logger.info("Calling URL FOR C2B ACK: (%s, %r) " % (url, sms_payload))
            output = requests.get(url, params=sms_payload, timeout=30)
            self.logger.info("Found result from sdp call: (%s) "
             % (output.text, ))
        except Exception as e:    # This is the correct syntax
            self.logger.error("Exception attempting to send BULK MPESA "\
                "message : %r :: %r " % (payload, e))
            output = None

        return output

    def award_bonus_to_profile(self, profile_id,msisdn, deposit_amount):
        enable_bonus = self.shikabet_configs['award_bonus_on_deposit']
        self.logger.info("Enable bonus : %r,  %r" % (enable_bonus, deposit_amount))
        if enable_bonus == 0 or enable_bonus == "0":
            return False
        bonus_amount_str = self.shikabet_configs['mpesa_deposit_bonus_amount']
        bonuses = bonus_amount_str.split(",") or []
        bonus_award = 0.0;
        for b in bonuses:
            threshhold, bonus = b.split(":")
            if float(deposit_amount) >= float(threshhold):
                bonus_award = bonus
                break
        if bonus_award == 0.0:
            return 0.0

        bonus_Q = """select bonus_amount from profile_bonus where profile_id=:pf and referred_msisdn=:rmsisdn
        and date_created > :dc limit 1"""
        rs = self.engine.execute(sql_text(bonus_Q),
            {'rmsisdn':msisdn,'pf':profile_id, 'dc':time.strftime("%Y-%m-%d")}).fetchone()

        if rs and rs[0]:
            self.logger.info("Bonus already awarded .. skipping bonus process..")
            return False

        connection = self.engine.connect()
        trans = connection.begin()
        try:
            profile_bonus_dict = {
                "profile_id": profile_id,
                "referred_msisdn": msisdn, #bonus on same number
                "bonus_amount":float(bonus_award),
                "status":'CLAIMED',
                "bet_on_status": 1,
                "expiry_date":datetime.now()+timedelta(days =1),
                "date_created": datetime.now(),
                "updated":datetime.now(),
                "created_by":"mpesa_deposit"
            }
            connection.execute(ProfileBonu.__table__.insert(),profile_bonus_dict)
            #profile_bonus_id = result_proxy.inserted_primary_key

            bonus_update_ql =\
            """update profile_balance set bonus_balance = (bonus_balance + %0.2f)
            where profile_id = :pf""" % float(bonus_award)
            self.logger.info("bonus update query %s" % bonus_update_ql)
            connection.execute(sql_text(bonus_update_ql), {'pf':profile_id})
            trans.commit()
            return bonus_award
        except Exception, e:
             self.logger.error("Exception trying to award bonus: %r" % e)
             trans.rollback()
             return 0

    def get_profile(self, msisdn):
        sql = "select profile_id from profile where msisdn=:msisdn limit 1"
        print self.db
        result = self.engine.execute(sql_text(sql), {'msisdn':msisdn}).fetchone()
        if result and result[0]:
            self.profile_id = result[0]
            return self.profile_id
        return None
    def is_valid_phone(self, ref):
       return self.clean_msisdn(ref)
        

    def deposit(self, message):
        payment_id = message.get("payment_id")
        #params payment_id, network, reference_id, msisdn, amount, receipt_number
        self.logger.error("FOUND PAYMENT NUMBER : %r::%r " % (payment_id, message))
        if not payment_id:
            self.logger.error("Invalid deposit message : %r " % message)
            return True
        if message.get('receipt_number') is None:
            self.logger.error('Invalid receipt number %r' % message.get('receipt_number'))
            return True
        account_number = self.clean_msisdn(message.get("msisdn"))
        ref = message.get("reference_id")
        ref_is_msisdn = self.is_valid_phone(ref)
        if ref_is_msisdn:
            account_number = self.clean_msisdn(ref)

        profile_id = self.get_profile(account_number)

        response = self.create_deposit_trx(message, profile_id, account_number)

        if response == "Error":
            self.logger.info("Intergrity Error ignoring ... trx")
            return True

        if response:
            #attempt award bonus
            amount = float(str(message.get("amount")))
            bonus_on_deposit = self.award_bonus_to_profile(profile_id, message.get("msisdn"), amount)
            name = ""
            balance, bonus_balance = self.get_account_balance({'msisdn':
                 message.get('msisdn')})
            print "%s::%s" % (balance, bonus_balance)
            for kycinfo in message.get('KYCInfo', []):
                name = name + kycinfo.get('KYCValue')
                break
            bonus_text = ""
            if bonus_on_deposit == False:
                bonus_text = ""
            elif bonus_on_deposit > 0.0:
                bonus_text = "You have earned a bonus of Tshs.%0.2f" % float(bonus_on_deposit)
            else:
                bonus_text ="Deposit Tshs 200 or above earn 50bob Bonus."

            if self.bet_expired:
                resp_text = "Hi %(title)s, Tshs %(amount)0.2f received, "\
                    "REF:%(ref)s. Could not settle bet expired bet ID %(bet_id)s,"\
                    " your deposit has been credited to your account. "\
                    " SHIKABETSPORT Bal %(bal)0.2f. Bet responsibly"

                resp_text = "Kiasi cha TZS %(amount)0.2f kimepokelewa kwenye "\
                    "akaunti yako. Bet yako yenye nambari ya utambulisho %(bet_id)s"\
                    " haijafanikiwa,fedha uliyolipia imerudishwa kwenye akaunti"\
                    " yako,salio lako jipya ni TZS %(bal)0.2f."

            elif self.less_bet_amount:
                resp_text = "Hi %(title)s, Tshs %(amount)s received, "\
                    "REF: %(ref)s. Could not settle bet ID %(bet_id)s. "\
                    "Received amount is less bet amount. "\
                    "Your deposit has been credited to your ACC. "\
                    "SHIKABETSPORT Bal %(bal)0.2f. Bet responsibly" 

                resp_text = "Kiasi cha TZS %(amount)s kimepokelewa kwenye "\
                    "akaunti yako, bet yenye nambari ya utambulisho %(bet_id)s"\
                    "imeshindwa kukamilika,kiasi cha fedha kilichowekwa kwenye "\
                    "hakitoshi ,fedha imerudishwa kwenye akaunti yako ya "\
                    "Shikabet, salio lako ni TZS %(bal)0.2f"
            elif self.bet_already_settled:
                resp_text = "Hi %(title)s, Tshs %(amount)s received, "\
                    "REF: %(ref)s. Could not settle bet ID %(bet_id)s. "\
                    "Bet already settled "\
                    "Your deposit has been credited to your ACC. "\
                    "SHIKABETSPORT Bal %(bal)0.2f. Bet responsibly"
                resp_text = "Kiasi cha TZS %(amount)0.2f kimepokewa,"\
                   " Bet yenye namba ya"\
                   " utambulisho %(bet_id)s imethibitishwa tayari."\
                   " Fedha imerudishwa kwenye akaunti yako. Salio lako "\
                   " ni TZS %(bal)0.2f."

            elif self.bet_settled:
                resp_text = "Hi %(title)s, Tshs %(amount)s received, REF: %(ref)s. "\
                    "BET ID %(bet_id)s. accepted. Possible win %(pwin)0.2f "\
                    "SHIKABETSPORT Bal %(bal)0.2f. Bet responsibly" 
                resp_text = "Kiasi cha TZS %(amount)0.2f kimepokewa,  Bet yenye namba ya"\
                   " utambulisho %(bet_id)s imethibitishwa, Kiasi cha ushindi ni"\
                   " TZS %(pwin)0.2f, salio kwenye akaunti yako ya "\
                   " Shikabet ni TZS %(bal)0.2f."
            else:
               resp_text = "Hi %(title)s, Tshs %(amount)s received, "\
                   "REF: %(ref)s. SHIKABETSPORT Bal %(bal)0.2f. Bet responsibly" 
               resp_text = "Umepokea Tzs %(amount)0.2f , salio kwenye "\
                   " akaunti yako ya Shikabet ni TZS %(bal)0.2f"

            send_message_norm = {
                "message": resp_text % { 'title':name.title(),
                    'amount':float(message.get('amount')), 
                    'ref':message.get('receipt_number'),
                    'bet_id':message.get('reference_id'),
                    'pwin': self.pwin, 'bal':float(balance) },
                'network':message.get('network'),
                "refNo": message.get('receipt_number'),
                "msisdn": message.get('msisdn'),
            }

            send_message = send_message_norm

            #handling referral bonus
            ref_query = """select bonus_amount, expiry_date, profile_id  from profile_bonus where referred_msisdn = :rmsisdn
             and status = :st"""
            ref_result = self.engine.execute(sql_text(ref_query), {'rmsisdn':message.get('msisdn'), 'st':'NEW'}).fetchone()
            self.logger.info("Ref bonus Q: %s, %r" % (ref_query, ref_result) )
            if ref_result and ref_result[0]:
                self.logger.info("Found bonus profile to reward")
                bonused = False
                self.logger.info("Bonus for profile: (%s ) " % ref_result[0])
                if ref_result[1] > datetime.now():
                    if float(str(message.get('amount'))) < 50:
                        status = 'NEW'
                    else:
                        status = 'CLAIMED'
                else:
                    status = 'EXPIRED'
                self.logger.info("Collectred status, %r " % status)

                try:
                    sql = """update profile_balance set bonus_balance = (bonus_balance+%0.2f) where
                    profile_id = :pf limit 1""" % (ref_result[0])

                    sql_b = """update profile_bonus set status='CLAIMED' where profile_id=:p
                    and status='NEW' and referred_msisdn = :rmsisdn"""

                    self.logger.info("Bonus P update Q: %r, %r" % (sql, ref_result[2]))
                    self.logger.info("Bonus R update Q: %r, %r" % (sql_b,
                    {'p':ref_result[2],"rmsisdn":message.get('msisdn')}))

                    connection = self.engine.connect()
                    trans = connection.begin()
                    connection.execute(sql_text(sql), {'pf':ref_result[2]})
                    connection.execute(sql_text(sql_b), {'p':ref_result[2],
                    "rmsisdn":message.get('MSISDN')})
                    trans.commit()

                    self.engine.execute(sql_text(sql), {'pf':ref_result[2]})
                    bonused = True
                except Exception, e:
                    trans.rollback()
                    self.logger.info("Bonus for profile error: (%r ) " % e)
                    pass
                if bonused and status =='CLAIMED':
                    self.logger.info("CLAIMED and BONUSED ...")
                    self.send_bonus_redeem_ack(ref_result[2], status, message.get('msisdn'))
                else:
                    self.logger.info("NOT BONUSED ... sending regret")
                    self.send_bonus_redeem_ack(ref_result[2], status, message.get('msisdn'),False)

            return self.send_mpesa_deposit_ack(send_message)
        return None

    def save_trivia_code(self, promo_code, mpesa_trx_id):
        mpesatrx = self.db_session.query(MpesaTransaction).\
        filter_by(mpesa_code=mpesa_trx_id).first()
        if mpesatrx:
            mpesatrx.promo_code = promo_code.upper()
            return True
        return False

    def get_b2c_password(self, payment_id, password, msisdn, amount):
        import base64
        import hashlib, binascii
        digest = "%s%s%s%s" % (payment_id, password, msisdn, int(amount))
        self.logger.info("DIGEST ==> %s"  % digest)
        dk = hashlib.sha256(digest).digest()
        hex_key = binascii.hexlify(dk).upper()
        ke_str = base64.b64encode(bytes(hex_key))
        return ke_str
 
    def clean_msisdn(self, msisdn):
        _msisdn = re.sub(r"\s+", '', msisdn)
        res = re.match('^(?:\+?(?:[1-9]{3})|0)?([0-9]{9})$', _msisdn)
        if res:
           return "255" + res.group(1)
        return None

    # FIX ME: Learn something to do with array_walk, burger!
    def get_network(self, msisdn):
        clean = self.clean_msisdn(msisdn)
        if clean[0:5] in ['25571', '25565', '25567']: 
            network = 'TIGO'
        elif clean[0:5] in ['25575', '25576', '25574']:
            network = 'VODACOM'
        elif clean[0:5] in ['25578', '25568', '25569']:
            network = 'AIRTEL'
        else:
            network='NONE'
        return network


    """
    Sample payload
    {
      "MSISDN": "254714423224",
      "amount": "100.00",
      "reference": "27",
      "id": "1ca3e212723940599c8bd3fc9e57436f7bae80cc",
      "user": { "MSISDN": "254714423224", "FirstName": "Chris",
           "LastName": "Mwirigi"}
    }
    curl -X POST -H "Authorization: Basic YWRtaW46YWRtaW4="
     http://localhost:8080/wallet/v1/withdraw
    """
    def request_withdrawal(self, json_data):
        url = self.withdraw_configs["url"]
        token = self.withdraw_configs["oath_token"]
        account_number = self.withdraw_configs["paybill_number"]

        headers = {
                 #'Authorization': 'Basic %s' % token,
                 'Accept': 'application/xml',
                 'Content-Type': 'text/plain'
                }
        hex_key = self.get_b2c_password(json_data.get('withdrawal_id'), token,
            json_data.get("msisdn"), json_data.get('amount'))
        
        payload = "<COMMAND><TYPE>DISBURSEMENT</TYPE>"\
            "<ACCOUNTNUMBER>%(paybill)s</ACCOUNTNUMBER>"\
            "<PAYMENTID>%(withdraw_id)s</PAYMENTID>"\
            "<MSISDN>%(msisdn)s</MSISDN>"\
            "<AMOUNT>%(amount)0.2f</AMOUNT>"\
            "<NETWORK>%(network)s</NETWORK>"\
            "<KEY>%(hex_key)s</KEY></COMMAND>" % {
                "msisdn": str(json_data.get("msisdn")),
                "amount": float(json_data.get("amount")),
                "withdraw_id": str(json_data.get('withdrawal_id')),
            	"hex_key": hex_key,
                "paybill":account_number,
                "network":self.get_network(json_data.get("msisdn"))
           }

        self.logger.info("Calling WITHDRAW URL: (%s, %s, %s) " % (url, payload, token))

        output = None

        try:
            profile_id = self.get_profile(json_data.get("msisdn"))
            w_q = "select amount, number_of_sends, status from withdrawal where withdrawal_id=:wid"
            res = self.engine.execute(sql_text(w_q), {'wid':json_data.get("withdrawal_id")}).fetchone()

            if not res or not res[0]:
                self.logger.info("Invalid withdrawal ignored: (%r) "
                 % (payload,))
                return True
            if res[2] in ['SUCCESS', 'REVERSED', 'FAILED']:
                self.logger.info("Invalid withdrawal status : (%r )" % (res[2]))
                return True

            sends = int(res[1]) if res[1] else 0
            number_of_sends = sends + 1

            if sends > 10:
                message = {'TransactionID': "%s-%s" % (json_data.get("withdrawal_id"),
                    time.strftime('%Y%m%d%H%M%S')), "ConversationID":
                         "%s-%s-%s" % (1, 1, json_data.get("withdrawal_id"))}
                self.logger.\
                info("Retry invoke withdraw API once more: (%r) "
                  % (message,))
                result = self.reverse_withdrawal_trx(message, profile_id)
                return True if result else False

            output = requests.post(url, data=payload, timeout=30, headers=headers)
            self.logger.info("Found result from sdp call: (%s) "
             % (output.text, ))
            if not output:
                self.logger.info("Failed to send real MPESA requeueing: (%r) "
                 % (json_text,))
                w_status = 'FAILED'
            else:
                xml = output.content
                import xmltodict
                try:
                    response = xmltodict.parse(xml)
                    success = response.get('COMMAND').get('RESPONSECODE') == str(200)
                except Exception, e:
                     self.logger.info("Can't pass response %r " % xml)
                     success = False
                if success:
                    w_status = 'SENT'
                    self.logger.info("MPESA WITHDRAWAL SUCCESS : (%r) "
                      % (payload,))
                else:
                    w_status = 'FAILED'
                    self.logger.info("MPESA WITHDRAWAL FAILED : (%r) "
                      % (payload,))
            w_update_Q = "update withdrawal set number_of_sends=:num, status=:st where withdrawal_id=:wid"
            self.engine.execute(sql_text(w_update_Q),
                {'num':number_of_sends, 'st':w_status, 'wid':json_data.get("withdrawal_id")})

            return w_status == 'SENT'


        except requests.exceptions.RequestException as e:    # This is the correct syntax
            self.logger.error("Exception attempting to send MO message : %r " % payload )
            output = None
        else:
            self.close()

        #we don't get status 200, well we failed we stage this for re-queue
        if output and output.status_code != 200:
            output = None

        return output

    def reverse_withdrawals(self, message):
        self.logger.info("reverse withdraw transaction %r" % message)

        if(int(message.get('ResultCode')) in [200, 0]):
            self.logger.info("withdraw request was successful %r" % message)
            #update withdraw status
            status = "SUCCESS"
            self.flag_withdrawal_status(message, status)
            return True
        if(int(message.get('ResultCode')) ==999):
            self.logger.info("Something @wallet dint add up for the transaction. %r"
             % message)
            result = self.reverse_withdrawal_trx(message, None)
            if result == "Error":
                self.logger.info("Intergrity Error ignoring reversal ... trx")
                return True
            if not result:
                return False

            if result == True:
                return True
            #send sms notification of to user
            amount, status, charge, profile_id, msisdn, network  = result
            message_str = {
                "msisdn": msisdn,
                "message": "Ndugu mteja TZS %0.2f uliotoa imerudishwa kwenye akaunti yako. Jaribu kutoa tena baadae." %
                    float(amount),
                "refNo": "%s_%s" % (msisdn, message.get('payment_id')),
            }
            self.logger.info("Sending message : %r" % message_str.get("message"))
            self.terminate_sendsms_api_message(message_str)
            return True
        return True

    def send_withdrawal_reverse_notification(self, json_data):
        self.logger.info("Called send_withdrawal_reverse_notification() ..")
        url = self.bulk_configs["url"]

        payload = {
            "msisdn": json_data.get('msisdn'),
            "message": json_data.get('message') or json_data.get('text'),
            "access_code": self.bulk_configs["access_code"],
            "linkid": json_data.get("refNo"),
            "short_code": self.bulk_configs["bulk_code"],
            "correlator": json_data.get("refNo"),
            "service_id": self.bulk_configs["service_id"]
        }

        outbox_id = self.outbox_message(payload)
        self.logger.info("Hitting outbox_message : %r " % outbox)

        if not outbox_id:
            self.logger.error("Failed to create outbox message : (%r) "
              % payload)
            return False
        self.logger.info("outbox not null : %s" % outbox_id)
        payload['correlator'] = outbox_id

        self.logger.info("Calling URL FOR C2B ACK: (%s, %r) "
          % (url, payload))
        output = None

        try:
            output = requests.post(url, data=payload, timeout=30)
            self.logger.info("Found result from sdp call: (%s) "
             % (output.text, ))
        except requests.exceptions.RequestException as e:    # This is the correct syntax
            self.logger.error("Exception attempting to send BULK MPESA \
message : %r " % payload)
            output = None

        return True

    def flag_withdrawal_status(self, message, status):
        try:
                #withdraw_data = message.get('ConversationID').split('-')
                withdrawId = message.get('payment_id') #withdraw_data[2]
                self.logger.error("Found withdraw ID for reversal ... %r "
                 % withdrawId)
        except Exception, e:
            self.logger.error("Missing withdral ID for flagging status %r "
             % e)
            return False

        q = """update withdrawal set status=:st, reference=:ref, provider_reference = :pref
        where withdrawal_id=:wid"""

        r = self.engine.execute(sql_text(q), {'st':status, 
            'ref':message.get('receipt_number'),
            'pref':message.get('receipt_number'), 'wid':message.get('payment_id')})

        if not r:
            self.logger.error("Missing withdral ID for flagging status\
 ... %r " % withdrawId)
            return True


    def reverse_withdrawal_trx(self, message, profile_id):
        try:
            try:
                withdrawId = message.get('payment_id')
                #withdrawId = withdraw_data[2]
                self.logger.error("Found withdraw ID %r and ref %r "
                 % (withdrawId, message.get('payment_id')))
            except Exception, e:
                self.logger.error("Missing withdral ID for reversal. %r " % e)
                return False

            withdrawal_sql = "select amount, withdrawal.status, charge, profile_id, profile.msisdn, profile.network from "\
                " withdrawal inner join profile on profile.msisdn=withdrawal.msisdn "\
                " where withdrawal_id=:wid limit 1"
            wrs = self.engine.execute(sql_text(withdrawal_sql), {'wid':withdrawId}).fetchone()
            status = wrs[1]
            profile_id = wrs[3]

            if not wrs or not wrs[0]:
                self.logger.error("Missing withdral ID for reversal in shikabet ... %r " % withdrawId)
                return True

            if str(wrs[1]) != 'SENT':
                self.logger.error("INVALID WITHDRAW Trx status %r " % status)

            #if not message.get('allow_success_status'):
            #    return True

            #amount of charge is the total transaction to be done
            amount = float(wrs[0]) + float(wrs[2])

            connection = self.engine.connect()
            trans = connection.begin()

            trx_credit = {
                    "profile_id":profile_id,
                    "account":"%s_%s" % (profile_id, 'VIRTUAL'),"iscredit":1,
                    "reference":str(message.get("payment_id")),"amount":amount,
                    "created_by":self.APP_NAME,"created":datetime.now(),"modified":datetime.now()
            }

            trx_credit_rs = connection.execute(Transaction.__table__.insert(), trx_credit)
            trx_credit_id = trx_credit_rs.inserted_primary_key

           
            withdrawal_sql = "update withdrawal set status = :st, reference=:ref, provider_reference=:pref where withdrawal_id=:wid limit 1"
            connection.execute(sql_text(withdrawal_sql), {'st':'FAILED', 'ref':message.get('receipt_number'),
            'pref':message.get('receipt_number'), 'wid':withdrawId})
            balanceq = "update profile_balance set balance=balance+%0.2f where profile_id =:pf limit 1 " % (amount,)
            connection.execute(sql_text(balanceq), {'pf':profile_id})
            trans.commit()

            self.logger.error("commited reverse transaction success")

            return wrs
        except IntegrityError, e:
            self.logger.error("Integrity Error skipping reverse trx ...%r" % e)
            trans.rollback()
            return "Error"
        except Exception, e:
            self.logger.error("Problem creating transaction message ...%r" % e)
            trans.rollback()
            return False
        else:
            self.close()

    """
    #params payment_id, network, reference_id, msisdn, amount, receipt_number
    """
    def create_deposit_trx(self, message, profile_id, account_number=None):
        self.bet_settled = self.less_bet_amount = self.bet_expired = self.bet_already_settled = False
        self.pwin=0
        try:
            if not profile_id:
                self.logger.info("Missing Payment Profile will create new ...%r, %r"
                 % (message, message.get("msisdn")))

                profile_dict = {
                    "msisdn":account_number,
                    "created":datetime.now(),
                    "modified":datetime.now(),
                    "status":1,
                    "network":self.get_network(account_number),
                    "created_by":self.APP_NAME,
                }
                result_proxy= self.engine.execute(Profile.__table__.insert(),profile_dict)
                profile_id = result_proxy.inserted_primary_key
                if not self.profile_id:
                    self.logger.error("Problem creating profile message ...%r" % profile_dict )
                    return False
        except Exception, e:
            self.logger.error("Problem collecting profile message ...%r, %r" % (message, e) )
            return False
        self.logger.info("created profile %r" % profile_id)
        try:
            #amount of charge is the total transaction to be done
            self.logger.info("Found C2B AMOUNT ...%r" % message.get("amount"))
            amount = float(str(message.get("amount")))
            name = ''
            if message.get('KYCInfo'):
                for kycinfo in message.get('KYCInfo'):
                    name = name + kycinfo.get('KYCValue') + " "

            connection = self.engine.connect()
            trans = connection.begin()
            transaction_time = datetime.now()
            if 'transaction_time' in message:
                self.logger.info("Looking for trx Time  ....")
                transaction_time  = datetime.strptime(message.get("transaction_time"), "%Y-%m-%d %H:%M:%S")

            mpesa_dict = {
                "msisdn":account_number or message.get("msisdn"), 
                "transaction_time":transaction_time,
                "message":message.get("receipt_number"),"account_no":message.get("reference_id") or "",
                "mpesa_code":message.get("receipt_number"),"mpesa_amt":amount,
                "mpesa_customer_id":message.get("reference_id") or "","mpesa_sender":name.title(),
                "business_number":message.get("payment_id"),"enc_params":"",
                "created":datetime.now(),
                "paid_by":message.get("msisdn")
            }
            self.logger.info("mpesa dict %r" % mpesa_dict)
            #mresult_proxy=\
            connection.execute(MpesaTransaction.__table__.insert(), mpesa_dict)
            #mpesa_trx_id = mresult_proxy.inserted_primary_key

            trx_credit_dict = {
                "profile_id":profile_id,"account":"%s_%s" % (profile_id, 'VIRTUAL'),
                "iscredit":1,"reference":message.get("receipt_number"),
                "amount":amount,"created_by":self.APP_NAME,
                "created":datetime.now(),"modified":datetime.now()
            }
            self.logger.info("trx_credit_dict dict %r" % trx_credit_dict)
            t1result_proxy= connection.execute(Transaction.__table__.insert(),trx_credit_dict)
            credit_trx_id = t1result_proxy.inserted_primary_key

            #check and update bet payment if present
            remainder = amount
            bet_id = message.get("reference_id")
            is_manual = int(message.get("manual", 0))
            append_str = " and bet.created > now() - interval 15 minute " if is_manual != 1 else ""
            if bet_id not in ['101010', '505050']:
                bet_q = "select bet.bet_id, bet.bet_amount, m.bet_closure,"\
                    " bet.possible_win, bet.status, if(jp.bet_id is null, 0, 1)jp_bet from bet "\
                    " inner join bet_slip bs on bs.bet_id = bet.bet_id "\
                    " inner join `match` m on m.parent_match_id = bs.parent_match_id "\
                    " left join jackpot_bet jp on jp.bet_id=bet.bet_id "\
                    " where bet.bet_id = :bet_id "
                bet_q += append_str

                res =  self.engine.execute(sql_text(bet_q),
                    {'bet_id':bet_id}).fetchall()
                if not res:
                    self.logger.info("Missing bet for bet_id %s from msisdn %s"
                        " on deposit" % (bet_id, message.get('msisdn') ))
                else:
                    expired = [d[2]for d in res if d[2] < transaction_time]
                    bet_amount = res[0][1]
                    self.pwin = res[0][3]
                    bet_status = res[0][4]
                    jp_bet = res[0][5]
                    #confirm the paid enough money
                    if bet_amount >  amount:
                        self.less_bet_amount = 1
                    elif len(expired):
                        self.bet_expired = 1
                    elif bet_status != 200:
                        # bet already settled ignore
                        self.bet_already_settled = 1
                    else:
                        st = 400 if not jp_bet else 9
                        q  = "update bet set status = :st, profile_id=:pf where bet_id = :bet_id limit 1"
                        q2 = "update transaction set status = :stat where reference =:bet_id"
                        q3 = "insert into ticket_settlement (profile_id, bet_id, trx_id, "\
                             "amount, created) values(:profile_id, :bet_id, :trx_id, "\
                             ":bet_amount, now())"
                        connection.execute(sql_text(q), {'bet_id':bet_id, 'st':st, 'pf':self.profile_id})
                        connection.execute(sql_text(q2), {'stat':'COMPLETE', 'bet_id':bet_id})
                        connection.execute(sql_text(q3), {'profile_id':self.profile_id, 
                           'bet_id':bet_id, 'trx_id':credit_trx_id, 'bet_amount':bet_amount
                            })
                        self.bet_settled = True
               	        remainder = float(amount) - float(bet_amount)

            profileUpdate = """INSERT INTO profile_balance(profile_id, balance,
            transaction_id, created) VALUES (:pf, :amount, :trx_id, NOW())
            ON DUPLICATE KEY UPDATE  balance = (balance+%0.2f)""" % (remainder, )

            connection.execute(sql_text(profileUpdate),
                {'pf':profile_id, 'amount':remainder, 'trx_id':-1})

            self.logger.info("profile balance update %r " % profileUpdate)
            trans.commit()
            return True
        except IntegrityError, e:
            self.logger.error("Integrity Error skipping mpesa trx save ...%r" % e)
            trans.rollback()
            return "Error"
        except Exception, e:
            self.logger.error("Problem creating transaction message ...%r" % e)
            trans.rollback()
            return False
        else:
            self.close()

    def get_account_balance(self, message):

        sql = """select balance, bonus_balance from profile_balance pb inner join profile p
            on pb.profile_id = p.profile_id where p.msisdn = :msisdn limit 1 """

        q = self.engine.execute(sql_text(sql),
             {'msisdn':message.get("MSISDN") or message.get("msisdn")}).fetchone()

        bal = None
        if q and q[0] is not None:
            return q[0], q[1]

        return 0, 0

    def get_roamtech_virtual_acc(self, acc):
        if acc == 'ROAMTECH_MPESA':
            if 'mpesa_roamtech_profile_id' in self.shikabet_configs:
                return self.shikabet_configs['mpesa_roamtech_profile_id']
            return 5

        if 'virtual_roamtech_profile_id' in self.shikabet_configs:
            return self.shikabet_configs['virtual_roamtech_profile_id']
        return 6


    def outbox_message(self, message):
        outbox = None
        try:
            self.logger.info("Found create outbox ...")
            outbox_dict = {
                "shortcode":self.bulk_configs["access_code"],"network":message.get("network"),
                "profile_id":self.profile_id,"linkid":message.get("linkid"),
                "retry_status":0,"sdp_id":self.bulk_configs["service_id"],
                "modified":datetime.now(),"text":message.get('message'),
                "msisdn":message.get("msisdn"),"date_created":datetime.now(),
                "date_sent":datetime.now(),
            }
            result_proxy= self.engine.execute(Outbox.__table__.insert(),outbox_dict)
            outbox_id = result_proxy.inserted_primary_key

            self.logger.info("Outbox commit success ...")
            return outbox_id
        except Exception, e:
            self.logger.error("Problem creating inbox message ...%r" % e)
            return None
        else:
            self.close()

