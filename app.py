import os
import psycopg2
from flask import Flask, request, redirect, url_for, render_template_string
from urllib.parse import urlparse
import string
import random
import hashlib

app = Flask(__name__)

# DB Config

DB_HOST = os.getenv('DB_HOST', 'postgresql-service')
DB_NAME = os.getenv('DB_NAME', 'k3surlshrt')
DB_USER = os.getenv('DB_USER', 'k3surlshrt')
DB_PASSWORD = os.getenv('DB_PASSWORD')


def get_db_connection():

    conn = psycopg2.connect(host=DB_HOST, database=DB_NAME,
                            user=DB_USER, password=DB_PASSWORD)
    return conn


def init_db():
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS urls (
                id SERIAL PRIMARY KEY,
                long_url TEXT NOT NULL,
                short_code VARCHAR(10) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()
        cur.close()
    except Exception as e:
        app.logger.error(f"Error initializing database: {e}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


def generate_short_code():
    while True:

        short_code = ''.join(random.choices(
            string.ascii_letters + string.digits, k=6))
        conn = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute(
                "SELECT short_code FROM urls WHERE short_code = %s", (short_code,))
            if not cur.fetchone():
                return short_code
        except Exception as e:
            app.logger.error(f"Error checking short code uniqueness: {e}")
        finally:
            if conn:
                conn.close()


@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        long_url = request.form['long_url']
        if not long_url:
            return "Please enter a URL", 400

        parsed_url = urlparse(long_url)
        if not parsed_url.scheme:
            long_url = "http://" + long_url

        conn = None
        short_code = None
        try:
            conn = get_db_connection()
            cur = conn.cursor()

            cur.execute(
                "SELECT short_code FROM urls WHERE long_url = %s", (long_url,))
            result = cur.fetchone()
            if result:
                short_code = result[0]
            else:
                short_code = generate_short_code()
                cur.execute(
                    "INSERT INTO urls (long_url, short_code) VALUES (%s, %s)", (long_url, short_code))
                conn.commit()
            return render_template_string(f'''
                <h1>URL Shortener</h1>
                <p>Original URL: {long_url}</p>
                <p>Short URL: <a href="/{short_code}">{request.url_root}{short_code}</a></p>
                <form method="POST">
                    <input type="text" name="long_url" placeholder="Enter a long URL" size="50">
                    <input type="submit" value="Shorten">
                </form>
            ''')
        except Exception as e:
            app.logger.error(f"Error processing URL: {e}")
            if conn:
                conn.rollback()
            return "Internal Server Error", 500
        finally:
            if conn:
                conn.close()
    return render_template_string('''
        <h1>URL Shortener</h1>
        <form method="POST">
            <input type="text" name="long_url" placeholder="Enter a long URL" size="50">
            <input type="submit" value="Shorten">
        </form>
    ''')


@app.route('/<short_code>')
def redirect_to_long_url(short_code):
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT long_url FROM urls WHERE short_code = %s", (short_code,))
        result = cur.fetchone()
        if result:
            return redirect(result[0])
        else:
            return "Short URL not found", 404
    except Exception as e:
        app.logger.error(f"Error redirecting: {e}")
        return "Internal Server Error", 500
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=5000)
