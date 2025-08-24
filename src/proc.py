import sqlite3
import requests
import dns.resolver
import itertools
import subprocess
import os
from pathlib import Path
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

FEED_URL = os.getenv("FEED_URL")
r = requests.get(FEED_URL, timeout=30, verify=False)
DB_FILE = "labor.db"
OUTPUT_FILE = "badhosts.txt"
RESOLVERS = ["1.1.1.1", "1.0.0.1", "8.8.8.8", "8.8.4.4"]

def fetch_feed():
    r = requests.get(FEED_URL, timeout=30)
    r.raise_for_status()
    return [line.strip() for line in r.text.splitlines() if line.strip()]

def check_soa(host, resolver_ip):
    try:
        resolver = dns.resolver.Resolver()
        resolver.nameservers = [resolver_ip]
        ans = resolver.resolve(host, "SOA")
        return bool(ans)
    except Exception:
        return False

def is_valid_host(host, rr_cycle):
    for resolver_ip in rr_cycle:
        if check_soa(host, resolver_ip):
            return True
    return False

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS hosts (hostname TEXT PRIMARY KEY)")
    conn.commit()
    return conn

def insert_hosts(conn, hosts):
    c = conn.cursor()
    c.executemany("INSERT OR IGNORE INTO hosts (hostname) VALUES (?)", [(h,) for h in hosts])
    conn.commit()

def export_hosts(conn):
    c = conn.cursor()
    c.execute("SELECT hostname FROM hosts ORDER BY hostname")
    rows = c.fetchall()
    with open(OUTPUT_FILE, "w") as f:
        for (h,) in rows:
            f.write(h + "\n")

def git_commit_and_push():
    subprocess.run(["git", "add", DB_FILE, OUTPUT_FILE], check=True)
    subprocess.run(["git", "commit", "-m", "Update labor.db and badhosts.txt"], check=False)
    subprocess.run(["git", "push"], check=True)

def main():
    feed_hosts = fetch_feed()
    rr_cycle = itertools.cycle(RESOLVERS)

    valid_hosts = []
    for host in feed_hosts:
        if is_valid_host(host, [next(rr_cycle)]):
            valid_hosts.append(host)

    conn = init_db()
    insert_hosts(conn, valid_hosts)
    export_hosts(conn)
    conn.close()

    git_commit_and_push()

if __name__ == "__main__":
    main()
