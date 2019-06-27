from flask import request, make_response
from flask_restful import Resource, reqparse
from flask import current_app
from Request import Process
import json


class ProcessQueue(Resource):
    def post(self):
        message_object = request.get_json()
        current_app.logger.info("Found Data: %r" % message_object)
        try:
            new_process = Process(current_app.logger, message_object, message=None)
            result = new_process.process_request()
        except Exception, e:
            result = None
            current_app.logger.error('FATAL Error processing queue message::::: %r, %r'% (message_object, e))
        if result:
            _response = make_response('Success', 200)
        else:
            _response = make_response('Failed processing request', 500)

        current_app.logger.info("Message process success with message:: %r ::api response:: %r " % (result, _response))

        return _response or make_response('Error', 500)


class Wekawekapesa(Resource):

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('msisdn', type=int, required=True,
                 help='Provide mobile number')
        parser.add_argument('amount', type=float, required=True,
                 help='Provide deposit amount')
        args = parser.parse_args(strict=True)
        current_app.logger.info("wekaweka pesa kwenye mpesa args %r" % args)
        process = Process(current_app.logger, None, message=None)
        mpon = process.call_mpesa_online(args['msisdn'], args['amount'])
        msg = "Failed"
        code = 400
	if args['amount'] > 9 and args['amount'] < 70001:
	    msg = "Wrong amount provided."
            code = 400
        if mpon:
            msg = "Success"
            code = 200
        resp = make_response(json.dumps(msg), code)
        return resp


class WekawekapesaDemo(Resource):

    def post(self):
        parser = reqparse.RequestParser()
        parser.add_argument('msisdn', type=int, required=True,
                 help='Provide mobile number')
        parser.add_argument('amount', type=float, required=True,
                 help='Provide deposit amount')
	parser.add_argument('reference', type=str, 
		 help='Provide mpesa reference, e.g Demo, My account etc.')
        args = parser.parse_args(strict=True)
        current_app.logger.info("wekaweka pesa kwenye mpesa demo args %r" % args)
        process = Process(current_app.logger, None, message=None)
        mpon = process.call_mpesa_online_demo(args['msisdn'], args['amount'], args['reference'])
        msg = "Failed"
        code = 400
        if args['amount'] > 9 and args['amount'] < 70001:
            msg = "Wrong amount provided."
            code = 400
        if mpon:
            msg = "Success"
            code = 200
        resp = make_response(json.dumps(msg), code)
        return resp

