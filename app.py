import io
import pandas as pd
import streamlit as st
from core import buscar_cnpjs_por_isin

st.set_page_config(page_title="CVM ISIN → CNPJs", layout="centered")

st.title("CVM – ISIN → CNPJs")
st.caption("Busca no CDA mais recente da CVM e retorna CNPJs associados ao ISIN.")

isin = st.text_input("Informe o ISIN", placeholder="Ex: BRBRKMDBS0A1").strip().upper()

col1, col2 = st.columns(2)
with col1:
    workers = st.slider("Paralelismo (threads)", 1, 6, 2)
with col2:
    buscar = st.button("Buscar")

if buscar:
    if not isin:
        st.error("Informe um ISIN válido.")
        st.stop()

    with st.spinner("Consultando dados da CVM..."):
        try:
            yyyymm, df_cnpjs, df_matches, df_errors = buscar_cnpjs_por_isin(
                isin, max_workers=workers
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
        file_name=f"resultado_{isin}_{yyyymm}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    with st.expander("Arquivos onde o ISIN apareceu"):
        st.dataframe(df_matches, use_container_width=True)

    if not df_errors.empty:
        with st.expander("Arquivos com erro"):
            st.dataframe(df_errors, use_container_width=True)