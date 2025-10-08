import requests
import io
import time
from datetime import datetime

# === Azure App Registration credentials ===
TENANT_ID = "aeae09de-052e-4237-87b7-0fd16388f59b"
CLIENT_ID = "06559164-e614-47bd-8f28-c2887a33d7f7"
CLIENT_SECRET = "rIV8Q~taiddiG3Syl4krqH~JgrgAhGpSin-MCaNc"

# === SharePoint/Graph details ===
SITE_HOSTNAME = "payrolleasy.sharepoint.com"
SITE_PATH = "sites/PowerBIDatabase"
DOCUMENT_LIBRARY = "Documents"

FOLDER_PATH_TENDERS = "Tussell API"
FOLDER_PATH_AWARDS = "Tussell API/Contract Awards"
FOLDER_PATH_SPEND = "Tussell API/Spend Data"
FOLDER_PATH_BUYERS = "Tussell API/Buyers"
FOLDER_PATH_SUPPLIERS = "Tussell API/Suppliers"
FOLDER_PATH_FRAMEWORKS = "Tussell API/Frameworks"

# === Tussell API details ===
TUSSELL_API_KEY = "e8cgb6qrbsydQm@6DBvmrzcHstGhcpxW"
TUSSELL_ENDPOINTS = {
    "tenders": "https://client.tussell.com/data/tenders",
    "awards": "https://client.tussell.com/data/awards",
    "spend": "https://client.tussell.com/data/spend",
    "buyers": "https://client.tussell.com/data/buyers",
    "suppliers": "https://client.tussell.com/data/suppliers",
    "frameworks": "https://client.tussell.com/data/frameworks",
}

# --------------------------
# 1️⃣ Get Microsoft Graph Token
# --------------------------
def get_graph_token():
    token_url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    token_data = {
        "grant_type": "client_credentials",
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope": "https://graph.microsoft.com/.default"
    }
    resp = requests.post(token_url, data=token_data)
    resp.raise_for_status()
    return resp.json()["access_token"]

# --------------------------
# 2️⃣ Get Drive ID of Document Library
# --------------------------
def get_drive_id(access_token):
    site_url = f"https://graph.microsoft.com/v1.0/sites/{SITE_HOSTNAME}:/{SITE_PATH}"
    resp = requests.get(site_url, headers={"Authorization": f"Bearer {access_token}"})
    resp.raise_for_status()
    site_id = resp.json()["id"]

    drive_url = f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives"
    resp = requests.get(drive_url, headers={"Authorization": f"Bearer {access_token}"})
    resp.raise_for_status()
    for d in resp.json()["value"]:
        if d["name"] == DOCUMENT_LIBRARY:
            return d["id"]

    raise Exception(f"Drive '{DOCUMENT_LIBRARY}' not found.")

# --------------------------
# 3️⃣ Ensure folder exists
# --------------------------
def ensure_folder_exists(access_token, drive_id, folder_path):
    folder_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}"
    resp = requests.get(folder_url, headers={"Authorization": f"Bearer {access_token}"})
    if resp.status_code == 404:
        create_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root/children"
        folder_data = {
            "name": folder_path.split("/")[-1],
            "folder": {},
            "@microsoft.graph.conflictBehavior": "replace"
        }
        parent_path = "/".join(folder_path.split("/")[:-1])
        if parent_path:
            create_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{parent_path}:/children"

        create_resp = requests.post(create_url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }, json=folder_data)
        create_resp.raise_for_status()
        print(f"📂 Created folder: {folder_path}")

# --------------------------
# 4️⃣ Download CSV from Tussell
# --------------------------
def download_tussell_csv(endpoint):
    headers = {"Authorization": f"Bearer {TUSSELL_API_KEY}", "Accept": "text/csv"}
    resp = requests.get(endpoint, headers=headers, stream=True)
    resp.raise_for_status()
    buf = io.BytesIO()
    for chunk in resp.iter_content(chunk_size=10 * 1024 * 1024):
        if chunk:
            buf.write(chunk)
    buf.seek(0)
    return buf

# --------------------------
# 5️⃣ Upload file to SharePoint (Resilient)
# --------------------------
def upload_large_file(access_token, drive_id, folder_path, filename, file_stream):
    ensure_folder_exists(access_token, drive_id, folder_path)

    def create_session():
        upload_session_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/root:/{folder_path}/{filename}:/createUploadSession"
        session_resp = requests.post(
            upload_session_url,
            headers={"Authorization": f"Bearer {access_token}"},
            json={"item": {"@microsoft.graph.conflictBehavior": "replace"}}
        )
        session_resp.raise_for_status()
        return session_resp.json()["uploadUrl"]

    upload_url = create_session()

    chunk_size = 5 * 1024 * 1024  # 5 MB
    file_stream.seek(0)
    file_size = len(file_stream.getbuffer())
    chunk_start = 0

    while chunk_start < file_size:
        chunk_end = min(chunk_start + chunk_size, file_size)
        chunk_data = file_stream.read(chunk_end - chunk_start)
        headers = {
            "Content-Length": str(chunk_end - chunk_start),
            "Content-Range": f"bytes {chunk_start}-{chunk_end - 1}/{file_size}"
        }

        for attempt in range(5):
            resp = requests.put(upload_url, headers=headers, data=chunk_data)
            if resp.status_code in (429, 503):
                wait_time = 2 ** attempt
                print(f"⚠️ Received {resp.status_code}. Retrying in {wait_time}s...")
                time.sleep(wait_time)
                continue
            elif resp.status_code >= 500:
                # re-create upload session for persistent 5xx errors
                print(f"⚠️ Server error {resp.status_code}. Creating new session...")
                upload_url = create_session()
                time.sleep(3)
                continue
            resp.raise_for_status()
            break
        else:
            raise Exception(f"❌ Upload failed after retries ({resp.status_code})")

        chunk_start = chunk_end

    print(f"✅ Uploaded {filename} ({file_size} bytes) → {folder_path}")

# --------------------------
# 6️⃣ Main Execution
# --------------------------
if __name__ == "__main__":
    print("🔐 Getting Microsoft Graph token...")
    token = get_graph_token()
    print("✅ Token acquired")

    print("📂 Fetching SharePoint drive ID...")
    drive_id = get_drive_id(token)
    print(f"✅ Drive ID: {drive_id}")

    datasets = [
        ("tenders.csv", FOLDER_PATH_TENDERS, TUSSELL_ENDPOINTS["tenders"]),
        ("awards.csv", FOLDER_PATH_AWARDS, TUSSELL_ENDPOINTS["awards"]),
        ("spend.csv", FOLDER_PATH_SPEND, TUSSELL_ENDPOINTS["spend"]),
        ("buyers.csv", FOLDER_PATH_BUYERS, TUSSELL_ENDPOINTS["buyers"]),
        ("suppliers.csv", FOLDER_PATH_SUPPLIERS, TUSSELL_ENDPOINTS["suppliers"]),
        ("frameworks.csv", FOLDER_PATH_FRAMEWORKS, TUSSELL_ENDPOINTS["frameworks"]),
    ]

    for filename, folder, endpoint in datasets:
        print(f"\n⬇️ Downloading {filename} from Tussell...")
        try:
            csv_buf = download_tussell_csv(endpoint)
            print(f"✅ Downloaded {filename} ({len(csv_buf.getbuffer())/1024:.1f} KB)")

            print(f"⬆️ Uploading {filename} to SharePoint...")
            upload_large_file(token, drive_id, folder, filename, csv_buf)
        except Exception as e:
            print(f"❌ Failed {filename}: {e}")

        print("⏳ Waiting 10 seconds before next upload...")
        time.sleep(10)

    print("\n🎉 All Tussell datasets uploaded successfully!")
