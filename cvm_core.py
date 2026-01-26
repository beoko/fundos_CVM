# cvm_core.py
import re, io, zipfile, requests, pandas as pd
import csv as pycsv
from typing import Set, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

pycsv.field_size_limit(10_000_000)

CDA_DIR_URL = "https://dados.cvm.gov.br/dados/FI/DOC/CDA/DADOS/"
HEADERS = {"User-Agent": "Mozilla/5.0"}

# ----------------- normalizadores -----------------
def norm_cols(cols: List[str]) -> List[str]:
    return [str(c).upper().strip().replace("\ufeff", "").replace("\r", "").replace("\n", "") for c in cols]

def norm_isin(x: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x).upper())

def norm_code(x: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(x).upper())

def norm_desc(x: str) -> str:
    x = str(x).upper().strip()
    x = re.sub(r"\s+", " ", x)
    x = re.sub(r"[^\w\s]", " ", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x

def norm_cnpj(x: str) -> str:
    return re.sub(r"\D", "", str(x))

# ----------------- util CVM -----------------
def listar_zips_disponiveis() -> List[str]:
    """
    Retorna lista de YYYYMM disponíveis no diretório CDA, em ordem decrescente (mais recente primeiro).
    """
    html = requests.get(CDA_DIR_URL, headers=HEADERS, timeout=60).text
    zips = re.findall(r'cda_fi_(\d{6})\.zip', html)
    if not zips:
        raise RuntimeError("Não encontrei arquivos ZIP no diretório da CVM.")
    return sorted(set(zips), reverse=True)

def url_zip(yyyymm: str) -> str:
    return f"{CDA_DIR_URL}cda_fi_{yyyymm}.zip"

def _get_cnpj_col(columns: List[str]) -> Optional[str]:
    cols = list(columns)
    if "CNPJ_FUNDO_CLASSE" in cols:
        return "CNPJ_FUNDO_CLASSE"
    cnpj_candidates = [c for c in cols if "CNPJ" in c]
    if not cnpj_candidates:
        return None
    classe_first = [c for c in cnpj_candidates if "CLASSE" in c]
    return classe_first[0] if classe_first else cnpj_candidates[0]

def _get_cd_ativo_cols(columns: List[str]) -> List[str]:
    cols = list(columns)
    out = []
    for name in ["CD_ATIVO", "CD_ATIV", "COD_ATIVO", "CODIGO_ATIVO"]:
        if name in cols:
            out.append(name)
    out += [c for c in cols if ("ATIV" in c and c.startswith("CD_") and c not in out)]
    return out

# ----------------- scanners (pandas + fallback csv) -----------------
def _scan_pandas(content: str, termo: str, modo: str) -> Tuple[Set[str], bool]:
    cnpjs: Set[str] = set()
    found = False

    df_iter = pd.read_csv(
        io.StringIO(content),
        sep=";",
        chunksize=100_000,
        engine="python",
        on_bad_lines="skip",
    )

    t_isin = norm_isin(termo)
    t_code = norm_code(termo)
    t_desc = norm_desc(termo)

    for chunk in df_iter:
        chunk.columns = norm_cols(chunk.columns)
        cnpj_col = _get_cnpj_col(chunk.columns.tolist())
        if not cnpj_col:
            continue

        if modo == "ISIN":
            isin_cols = [c for c in chunk.columns if "ISIN" in c]
            if not isin_cols:
                continue
            mask = False
            for col in isin_cols:
                mask |= (chunk[col].astype(str).apply(norm_isin) == t_isin)

        elif modo == "CDB_CODIGO":
            cd_cols = _get_cd_ativo_cols(chunk.columns.tolist())
            if not cd_cols:
                continue
            mask = False
            for col in cd_cols:
                mask |= (chunk[col].astype(str).apply(norm_code) == t_code)

        elif modo == "CDB_DESCR_EXATA":
            if "DS_ATIVO" not in chunk.columns:
                continue
            mask = (chunk["DS_ATIVO"].astype(str).apply(norm_desc) == t_desc)

        else:
            raise ValueError("modo inválido")

        sub = chunk[mask]
        if sub.empty:
            continue

        found = True
        vals = sub[cnpj_col].astype(str).apply(norm_cnpj).tolist()
        cnpjs.update(v for v in vals if v and v != "NAN")

    return cnpjs, found

def _scan_csv_fallback(content: str, termo: str, modo: str) -> Tuple[Set[str], bool]:
    cnpjs: Set[str] = set()
    found = False

    reader = pycsv.reader(io.StringIO(content), delimiter=";")
    header = norm_cols(next(reader, []))

    cnpj_col = _get_cnpj_col(header)
    if not cnpj_col:
        return set(), False
    cnpj_idx = header.index(cnpj_col)

    t_isin = norm_isin(termo)
    t_code = norm_code(termo)
    t_desc = norm_desc(termo)

    if modo == "ISIN":
        idxs = [i for i, h in enumerate(header) if "ISIN" in h]
        if not idxs:
            return set(), False
        for row in reader:
            if any(i < len(row) and norm_isin(row[i]) == t_isin for i in idxs):
                found = True
                if cnpj_idx < len(row):
                    c = norm_cnpj(row[cnpj_idx])
                    if c:
                        cnpjs.add(c)

    elif modo == "CDB_CODIGO":
        cols = _get_cd_ativo_cols(header)
        if not cols:
            return set(), False
        idxs = [header.index(c) for c in cols if c in header]
        for row in reader:
            ok = False
            for i in idxs:
                if i < len(row) and norm_code(row[i]) == t_code:
                    ok = True
                    break
            if ok:
                found = True
                if cnpj_idx < len(row):
                    c = norm_cnpj(row[cnpj_idx])
                    if c:
                        cnpjs.add(c)

    elif modo == "CDB_DESCR_EXATA":
        if "DS_ATIVO" not in header:
            return set(), False
        ds_idx = header.index("DS_ATIVO")
        for row in reader:
            if ds_idx < len(row) and norm_desc(row[ds_idx]) == t_desc:
                found = True
                if cnpj_idx < len(row):
                    c = norm_cnpj(row[cnpj_idx])
                    if c:
                        cnpjs.add(c)
    else:
        raise ValueError("modo inválido")

    return cnpjs, found

def _processar_arquivo(zip_bytes: bytes, filename: str, termo: str, modo: str):
    try:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            with zf.open(filename, "r") as f:
                content = f.read().decode("latin1", errors="replace")
        try:
            cnpjs, found = _scan_pandas(content, termo, modo)
            return cnpjs, found, None
        except Exception:
            cnpjs, found = _scan_csv_fallback(content, termo, modo)
            return cnpjs, found, None
    except Exception as e:
        return set(), False, str(e)

def _varrer_um_mes(yyyymm: str, termo: str, modo: str, max_workers: int) -> Tuple[Set[str], List[str], List[Tuple[str, str]]]:
    """
    Varre um ZIP (um mês). Retorna:
      - set cnpjs
      - lista de arquivos com match
      - lista de (arquivo, erro)
    """
    r = requests.get(url_zip(yyyymm), headers=HEADERS, timeout=240)
    r.raise_for_status()
    zip_data = r.content

    with zipfile.ZipFile(io.BytesIO(zip_data)) as zf:
        csv_files = [n for n in zf.namelist() if n.lower().endswith(".csv")]

    all_cnpjs: Set[str] = set()
    matches: List[str] = []
    errors: List[Tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futs = {ex.submit(_processar_arquivo, zip_data, name, termo, modo): name for name in csv_files}
        for fut in as_completed(futs):
            name = futs[fut]
            cnpjs, found, err = fut.result()
            if err:
                errors.append((name, err))
            elif found:
                matches.append(name)
                all_cnpjs.update(cnpjs)

    return all_cnpjs, matches, errors

# ----------------- API principal (multi-mês) -----------------
def buscar_cnpjs(ativo: str, categoria: str, max_workers: int = 2, meses: int = 12):
    """
    categoria:
      - 'CREDITO_PRIVADO'  => interpreta ativo como ISIN (match exato)
      - 'CDB'             => se ativo tem espaço => DESCRIÇÃO COMPLETA exata
                             senão => CÓDIGO DO ATIVO exato

    meses: quantos meses (ZIPs) tentar a partir do mais recente.

    Retorna:
      (ultimo_yyyymm_tentado, df_cnpjs, df_matches, df_errors, df_meses_com_match)
    """
    ativo = (ativo or "").strip()
    if not ativo:
        raise ValueError("Ativo vazio.")

    categoria = (categoria or "").strip().upper()
    if categoria not in ("CREDITO_PRIVADO", "CDB"):
        raise ValueError("Categoria inválida. Use 'CREDITO_PRIVADO' ou 'CDB'.")

    if categoria == "CREDITO_PRIVADO":
        modo = "ISIN"
    else:
        modo = "CDB_DESCR_EXATA" if (" " in ativo) else "CDB_CODIGO"

    max_workers = max(1, min(int(max_workers), 6))
    meses = max(1, min(int(meses), 60))  # limita p/ não explodir

    yyyymms = listar_zips_disponiveis()
    yyyymms = yyyymms[:meses]

    all_cnpjs: Set[str] = set()
    matches_rows: List[Tuple[str, str]] = []   # (yyyymm, arquivo)
    errors_rows: List[Tuple[str, str, str]] = []  # (yyyymm, arquivo, erro)
    meses_com_match: Set[str] = set()

    ultimo = yyyymms[-1]

    for yyyymm in yyyymms:
        cnpjs_mes, matches_mes, errors_mes = _varrer_um_mes(yyyymm, ativo, modo, max_workers=max_workers)

        if matches_mes:
            meses_com_match.add(yyyymm)
            for arq in matches_mes:
                matches_rows.append((yyyymm, arq))

        if errors_mes:
            for arq, err in errors_mes:
                errors_rows.append((yyyymm, arq, err))

        if cnpjs_mes:
            all_cnpjs.update(cnpjs_mes)

        # Se quiser “parar cedo” quando já achou algo, descomente:
        # if all_cnpjs:
        #     break

    df_cnpjs = pd.DataFrame(sorted([c for c in all_cnpjs if c]), columns=["CNPJ"])
    df_matches = pd.DataFrame(matches_rows, columns=["YYYYMM", "Arquivo"])
    df_errors = pd.DataFrame(errors_rows, columns=["YYYYMM", "Arquivo", "Erro"])
    df_meses_match = pd.DataFrame(sorted(list(meses_com_match), reverse=True), columns=["YYYYMM_com_match"])

    return ultimo, df_cnpjs, df_matches, df_errors, df_meses_match

# compatibilidade com o que você já tinha
def buscar_cnpjs_por_isin(isin_input: str, max_workers: int = 2, meses: int = 12):
    return buscar_cnpjs(isin_input, categoria="CREDITO_PRIVADO", max_workers=max_workers, meses=meses)
