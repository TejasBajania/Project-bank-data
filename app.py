# Main module for preparing APIs response
import requests
from flask import Flask
from flask import jsonify
from flask import request
from data_to_db import DataDB

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False


@app.route("/post_data", methods=["POST"])
def insert_data():
    try:
        print('---inserting data---')
        data_input = request.get_json()
        response = DataDB().read_and_update(data_input['csvPath']+data_input['csvFilename'])

        return jsonify({"message": "Data insertion {}".format(response)})

    except Exception as e:

        print(f'error at inserting data app: {e}')
        return jsonify({"message": str(e)}), 400


@app.route("/records", methods=["GET"])
def num_of_records():
    try:
        print('--- Fetching number of records ---')
        response = DataDB().records_count()

        return jsonify({"Records_count": "{}".format(response)})

    except Exception as e:

        print(f'error at fetching records app: {e}')
        return jsonify({"message": str(e)}), 400


@app.route("/banks", methods=["GET"])
def unique_banks():

    try:

        print('--- Fetching unique banks ---')
        response = DataDB().unique_banknames()

        return jsonify({"Unique_bank_names": response})

    except Exception as e:

        print(f'error at unique bank names app: {e}')
        return jsonify({"message": str(e)}), 400


@app.route("/<start_date>/<end_date>", methods=["GET"])
def date_response(start_date,end_date):

    try:

        data_input = request.get_json()
        response = DataDB().dateswise_data(
            start_date,
            end_date)
        return jsonify({"numOftransactions": response})

    except Exception as e:

        print(f'error at date response: {e}')
        return jsonify({"message": str(e)}), 400


@app.route("/customer_names", methods=["GET"])
def cust_names():

    try:

        response = DataDB().fetch_cust_names()
        return jsonify(response)

    except Exception as e:

        print(f'error at customer names: {e}')
        return jsonify({"message": str(e)}), 400


@app.route("/transactions_summary", methods=["GET"])
def trans_summary():

    try:

        response = DataDB().fetch_trans_summary()
        return jsonify({"TransSummary": response})

    except Exception as e:

        print(f'error at transactions summary: {e}')
        return jsonify({"message": str(e)}), 400


@app.route("/transaction_amount_summary", methods=["GET"])
def trans_amount_summary():

    try:

        response = DataDB().fetch_trans_amount_summary()
        return jsonify({"TransAmountSummary": response})

    except Exception as e:

        print(f'error at transactions amount summary: {e}')
        return jsonify({"message": str(e)}), 400


@app.route("/total_transaction_amount", methods=["GET"])
def total_amount():

    try:

        response = DataDB().fetch_total_amount()
        return jsonify({"totalAmount": response.values[0]})

    except Exception as e:

        print(f'error at total transactions amount: {e}')
        return jsonify({"message": str(e)}), 400


if __name__ == '__main__':

    print('--- Server is ready ---')
    app.run(host='0.0.0.0', threaded=True, debug=False)
