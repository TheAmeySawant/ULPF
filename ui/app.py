import streamlit as st
import os
import time

st.set_page_config(page_title="ULPF Simple Controller", page_icon="🛡️", layout="wide")
st.title("🛡️ ULPF Simple Ingestion Controller")
st.markdown("Deploy a preconfigured HTTP Webhook source to the Vector Data Plane.")
st.divider()

CONFIG_DIR = "/etc/vector/configs"
CONFIG_FILE = os.path.join(CONFIG_DIR, "custom_dummy_source.yaml")

# The preconfigured, hardcoded Vector YAML configuration
PRECONFIGURED_YAML = """
sources:
  src_dummy_webhook:
    type: http_server
    address: "0.0.0.0:8080"
    decoding:
      codec: json

transforms:
  map_dummy:
    type: remap
    inputs: [src_dummy_webhook]
    source: |-
      .ulpf.event_uuid = uuid_v4()
      .ulpf.ingestion_timestamp = now()
      .ulpf.normalization_status = "FULL"
      .schema_version = "1.0.0"
      .class_name = "Application Activity"
      .class_uid = 4001
      
      # Hardcoded field mappings for our test JSON
      .metadata.product.name = string!(.app_name)
      .activity_id = to_int!(.error_code)
      .src_endpoint.ip = string!(.customer_ip)

sinks:
  sink_dummy:
    type: kafka
    inputs: [map_dummy]
    bootstrap_servers: "ulpf-streaming-bus:9092"
    topic: "ocsf-events"
    encoding:
      codec: json
"""

col1, col2 = st.columns(2)

with col1:
    st.subheader("1. Deploy Source")
    if st.button("🚀 Deploy Dummy Webhook (Port 8080)", type="primary", use_container_width=True):
        with st.spinner("Writing configuration to Data Plane..."):
            # Write the static YAML to the shared volume
            with open(CONFIG_FILE, "w") as f:
                f.write(PRECONFIGURED_YAML.strip())
            time.sleep(1) # Give Vector a moment to hot-reload
            
        st.success("✅ Preconfigured YAML deployed! Vector has hot-reloaded.")
        
    if os.path.exists(CONFIG_FILE):
        if st.button("🛑 Revoke Dummy Source", use_container_width=True):
            os.remove(CONFIG_FILE)
            st.warning("Configuration deleted. Vector has hot-reloaded and closed port 8080.")
            st.rerun()

with col2:
    st.subheader("2. Test Payload")
    st.info("Once deployed, run this command in your terminal to test the ingestion:")
    st.code("""curl -X POST http://localhost:8080/ \\
     -H "Content-Type: application/json" \\
     -d '{"app_name": "SIH-Payment", "error_code": 503, "message": "Timeout", "customer_ip": "114.22.33.10"}'""", language="bash")
