# EmpApp.py  (fixed)
from flask import Flask, render_template, request
import pymysql
import os
import boto3
from config import *   # keep temporarily; move to env vars / secrets in production

app = Flask(__name__)

# Use the values from config.py
BUCKET = custombucket
REGION = customregion
DB_HOST = customhost
DB_USER = customuser
DB_PASS = custompass
DB_NAME = customdb
DB_PORT = 3306
TABLE = 'employee'


def get_db_connection():
    """Create a new DB connection for each request (safer than a global connection)."""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        db=DB_NAME,
        port=DB_PORT,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False
    )


@app.route("/", methods=['GET', 'POST'])
def home():
    return render_template('AddEmp.html')


@app.route("/addemployee", methods=['GET', 'POST'])
def AddEmployee():
    return render_template('AddEmp.html')


@app.route("/getemployee", methods=['GET', 'POST'])
def GetEmployee():
    return render_template('GetEmp.html')


@app.route("/fetchdata", methods=['POST'])
def GetEmp():
    emp_id = request.form.get('emp_id')
    select_sql = "SELECT * FROM employee WHERE empid = %s"

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # pass a single-element tuple (note trailing comma)
            cursor.execute(select_sql, (emp_id,))
            myresult = cursor.fetchall()
    except Exception as e:
        # for debugging return error text; remove or log in production
        return f"DB error: {str(e)}"
    finally:
        if conn:
            conn.close()

    return render_template('GetEmpOutput.html', name=myresult)


@app.route("/addemp", methods=['POST'])
def AddEmp():
    # get form fields (use .get to avoid KeyError)
    emp_id = request.form.get('emp_id')
    first_name = request.form.get('first_name')
    last_name = request.form.get('last_name')
    pri_skill = request.form.get('pri_skill')
    location = request.form.get('location')
    emp_image_file = request.files.get('emp_image_file')

    if not emp_image_file or emp_image_file.filename == "":
        return "Please select a file"

    # use explicit column names; adjust if your table columns are named differently
    insert_sql = """
        INSERT INTO employee (empid, firstname, lastname, pri_skill, location)
        VALUES (%s, %s, %s, %s, %s)
    """

    conn = None
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(insert_sql, (emp_id, first_name, last_name, pri_skill, location))
        conn.commit()
        emp_name = f"{first_name} {last_name}"

        # upload to S3 using upload_fileobj (works with Flask's file object)
        s3_client = boto3.client('s3', region_name=REGION)
        emp_image_file.stream.seek(0)
        s3_key = f"emp-id-{emp_id}_image_file"

        try:
            s3_client.upload_fileobj(emp_image_file.stream, BUCKET, s3_key)
            # build an object URL (region handling)
            bucket_loc = s3_client.get_bucket_location(Bucket=BUCKET)
            loc = bucket_loc.get('LocationConstraint')
            if loc is None:
                s3_host = "s3.amazonaws.com"
            else:
                s3_host = f"s3-{loc}.amazonaws.com"
            object_url = f"https://{s3_host}/{BUCKET}/{s3_key}"
            print("Uploaded image to:", object_url)
        except Exception as e:
            return f"S3 upload error: {str(e)}"

    except Exception as e:
        if conn:
            conn.rollback()
        return f"DB insert error: {str(e)}"
    finally:
        if conn:
            conn.close()

    return render_template('AddEmpOutput.html', name=emp_name)


if __name__ == '__main__':
    # dev server only
    app.run(host='0.0.0.0', port=8080, debug=True)
