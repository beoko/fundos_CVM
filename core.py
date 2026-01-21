# core.py
import re, io, zipfile, requests, pandas as pd
import csv as pycsv
from typing import Set, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

pycsv.field_size_limit(10_000_000)

CDA_DIR_URL = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def norm_cols(cols: List[str]) -> List[str]:
    return [str(c).upper().strip().replace("\ufeff", "").replace("\r", "").replace("\n", "") for c in cols]

def norm_isin(x: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x).upper())

def norm_cnpj(x: str) -> str:
    return re.sub(r"\D", "", str(x))

def descobrir_zip_mais_recente() -> Tuple[str, str]:
    html = requests.get(CDA_DIR_URL, headers=HEADERS, timeout=60).text
    zips = re.findall(r'cda_fi_(\d{6})\.zip', html)
    if not zips:
        raise RuntimeError("Não encontrei arquivos ZIP no diretório da CVM.")
    yyyymm = max(zips)
    return yyyymm, f"{CDA_DIR_URL}cda_fi_{yyyymm}.zip"

def _scan_pandas(content: str, isin_target: str) -> Tuple[Set[str], bool]:
    cnpjs: Set[str] = set()
    found = False

    df_iter = pd.read_csv(
        io.StringIO(content),
        sep=";",
        chunksize=100_000,
        engine="python",
        on_bad_lines="skip",
    )

    for chunk in df_iter:
        chunk.columns = norm_cols(chunk.columns)

        isin_cols = [c for c in chunk.columns if "ISIN" in c]
        if not isin_cols:
            continue

        mask = False
        for col in isin_cols:
            mask |= (chunk[col].astype(str).apply(norm_isin) == isin_target)

        sub = chunk[mask]
        if sub.empty:
            continue

        found = True

        # Preferir CNPJ_FUNDO_CLASSE; fallback: qualquer coluna com CNPJ
        cnpj_col = None
        if "CNPJ_FUNDO_CLASSE" in sub.columns:
            cnpj_col = "CNPJ_FUNDO_CLASSE"
        else:
            cnpj_candidates = [c for c in sub.columns if "CNPJ" in c]
            if cnpj_candidates:
                classe_first = [c for c in cnpj_candidates if "CLASSE" in c]
                cnpj_col = classe_first[0] if classe_first else cnpj_candidates[0]

        if cnpj_col:
            vals = sub[cnpj_col].astype(str).apply(norm_cnpj).tolist()
            cnpjs.update(v for v in vals if v and v != "NAN")

    return cnpjs, found

def _scan_csv_fallback(content: str, isin_target: str) -> Tuple[Set[str], bool]:
    cnpjs: Set[str] = set()
    found = False

    reader = pycsv.reader(io.StringIO(content), delimiter=';')
    header = norm_cols(next(reader, []))

    isin_idxs = [i for i, h in enumerate(header) if "ISIN" in h]
    if not isin_idxs:
        return set(), False

    # Preferir CNPJ_FUNDO_CLASSE; fallback: qualquer coluna com CNPJ
    cnpj_idx = None
    if "CNPJ_FUNDO_CLASSE" in header:
        cnpj_idx = header.index("CNPJ_FUNDO_CLASSE")
    else:
        cnpj_candidates = [i for i, h in enumerate(header) if "CNPJ" in h]
        if cnpj_candidates:
            classe_first = [i for i in cnpj_candidates if "CLASSE" in header[i]]
            cnpj_idx = classe_first[0] if classe_first else cnpj_candidates[0]

    for row in reader:
        if not row:
            continue

        matched = False
        for i in isin_idxs:
            if i < len(row) and norm_isin(row[i]) == isin_target:
                matched = True
                break
        if not matched:
            continue

        found = True
        if cnpj_idx is not None and cnpj_idx < len(row):
            c = norm_cnpj(row[cnpj_idx])
            if c:
                cnpjs.add(c)

    return cnpjs, found

def _processar_arquivo(zip_bytes: bytes, filename: str, isin_target: str) -> Tuple[Set[str], bool, Optional[str]]:
    """
    Processa um CSV do ZIP.
    Retorna: (cnpjs, found_any, error_message)
    """
    try:
        # Reabrir ZIP por thread (thread-safe)
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            with zf.open(filename, "r") as f:
                content = f.read().decode("latin1", errors="replace")

        try:
            cnpjs, found = _scan_pandas(content, isin_target)
            return cnpjs, found, None
        except Exception:
            cnpjs, found = _scan_csv_fallback(content, isin_target)
            return cnpjs, found, None

    except Exception as e:
        return set(), False, str(e)

def buscar_cnpjs_por_isin(isin_input: str, max_workers: int = 2):
    """
    Função para o Streamlit.
    Retorna: (yyyymm, df_cnpjs, df_matches, df_errors)
    """
    isin_target = norm_isin(isin_input)
    if not isin_target:
        raise ValueError("ISIN vazio/inválido.")

    yyyymm, zip_url = descobrir_zip_mais_recente()

    r = requests.get(zip_url, headers=HEADERS, timeout=240)
    r.raise_for_status()
    zip_data = r.content  # (ok para MVP)

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]

    all_cnpjs: Set[str] = set()
    matches: List[str] = []
    errors: List[Tuple[str, str]] = []

    # paralelismo moderado para Streamlit Cloud
    max_workers = max(1, min(int(max_workers), 6))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_processar_arquivo, zip_data, name, isin_target) for name in csv_files]

        for name, fut in zip(csv_files, futures):
            cnpjs, found, err = fut.result()
            if err:
                errors.append((name, err))
            elif found:
                matches.append(name)
                all_cnpjs.update(cnpjs)

    cnpjs_final = sorted([c for c in all_cnpjs if c and c != "NAN"])

    df_cnpjs = pd.DataFrame(cnpjs_final, columns=["CNPJ"])
    df_matches = pd.DataFrame(matches, columns=["Arquivo"])
    df_errors = pd.DataFrame(errors, columns=["Arquivo", "Erro"])

    return yyyymm, df_cnpjs, df_matches, df_errors
