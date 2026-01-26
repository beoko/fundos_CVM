import io
import pandas as pd
import streamlit as st
from core import buscar_cnpjs  # <- trocou aqui

st.set_page_config(page_title="CVM Ativo → CNPJs", layout="centered")

st.title("CVM – Ativo → CNPJs")
st.caption("Busca no CDA mais recente da CVM e retorna CNPJs associados ao ativo (Crédito Privado via ISIN ou CDB).")

# <- novo: escolha de categoria
categoria_ui = st.radio(
    "Tipo de ativo",
    ["Crédito Privado (ISIN)", "CDB"],
    horizontal=True
)
categoria = "CREDITO_PRIVADO" if "ISIN" in categoria_ui else "CDB"

# <- novo: um único input de ativo (ISIN ou CDB)
ativo = st.text_input(
    "Informe o ativo",
    placeholder="Crédito: BRBRKMDBS0A1 | CDB: CDB2236XODL (código) ou CDB PRE DU CDB2236XODL (descrição completa)"
).strip().upper()

col1, col2 = st.columns(2)
with col1:
    workers = st.slider("Paralelismo (threads)", 1, 6, 2)
with col2:
    buscar = st.button("Buscar")

if buscar:
    if not ativo:
        st.error("Informe um ativo válido.")
        st.stop()

    with st.spinner("Consultando dados da CVM..."):
        try:
            # <- trocou aqui: chama buscar_cnpjs com categoria
            yyyymm, df_cnpjs, df_matches, df_errors = buscar_cnpjs(
                ativo, categoria=categoria, max_workers=workers
            )
        except Exception as e:
            st.error(f"Erro na execução: {e}")
            st.stop()

    st.success(f"Consulta finalizada — CDA {yyyymm}")
    st.metric("CNPJs encontrados", len(df_cnpjs))

    st.subheader("CNPJs")
    st.dataframe(df_cnpjs, use_container_width=True)

    # Download Excel
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_cnpjs.to_excel(writer, index=False, sheet_name="CNPJs")
        df_matches.to_excel(writer, index=False, sheet_name="Arquivos_com_match")
        if not df_errors.empty:
            df_errors.to_excel(writer, index=False, sheet_name="Erros")

    st.download_button(
        "📥 Baixar Excel",
        data=output.getvalue(),
        file_name=f"resultado_{ativo}_{categoria}_{yyyymm}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    with st.expander("Arquivos onde o ativo apareceu"):
        st.dataframe(df_matches, use_container_width=True)

    if not df_errors.empty:
        with st.expander("Arquivos com erro"):
            st.dataframe(df_errors, use_container_width=True)
