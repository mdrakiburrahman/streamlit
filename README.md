# Streamlit

```powershell
$GIT_ROOT = git rev-parse --show-toplevel

python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

## Kusto Pinger

```powershell
streamlit run kusto_pinger.py -- --clusters "https://eventhousekusto.westcentralus.kusto.windows.net:kusto,https://trd-61q0f5tpkwer4f34bh.z6.kusto.fabric.microsoft.com:kusto" --poll 5
```
