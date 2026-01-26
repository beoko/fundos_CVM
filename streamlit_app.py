import io
import pandas as pd
import streamlit as st
import cvm_core as core

# ----------------- Configuração da página -----------------
st.set_page_config(
    page_title="CVM – Ativo → CNPJs",
    layout="centered"
)

st.title("CVM – Ativo → CNPJs")
st.caption(
    "Busca no CDA mais recente da CVM e retorna CNPJs associados ao ativo. "
    "Suporta Crédito Privado (ISIN) e CDB (código ou descrição completa)."
)

# ----------------- Seleção do tipo de ativo -----------------
categoria_ui = st.radio(
    "Tipo de ativo",
    ["Crédito Privado (ISIN)", "CDB"],
    horizontal=True
)

categoria = "CREDITO_PRIVADO" if "ISIN" in categoria_ui else "CDB"

# ----------------- Input do ativo -----------------
ativo = st.text_input(
    "Informe o ativo",
    placeholder="ISIN: BRBRKMDBS0A1 | CDB: CDB2236XODL ou CDB PRE DU CDB2236XODL"
).strip().upper()

# ----------------- Controles -----------------
col1, col2, col3 = st.columns(3)

with col1:
    workers = st.slider("Paralelismo (threads)", 1, 6, 2)

with col2:
    meses = st.slider("Meses a consultar", 1, 36, 12)

with col3:
    buscar = st.button("Buscar")


# ----------------- Execução -----------------
if buscar:
    if not ativo:
        st.error("Informe um ativo válido.")
        st.stop()

    with st.spinner("Consultando dados da CVM... (pode demorar ao varrer vários meses)"):
        try:
            ultimo_yyyymm, df_cnpjs, df_matches, df_errors, df_meses_match = core.buscar_cnpjs(
                ativo, categoria=categoria, max_workers=workers, meses=meses
            )
        except Exception as e:
            st.error(f"Erro na execução: {e}")
            st.stop()

    st.success(f"Consulta finalizada — últimos {meses} meses (até {ultimo_yyyymm})")
    st.metric("CNPJs encontrados", len(df_cnpjs))

    if not df_meses_match.empty:
        st.caption(f"Meses com ocorrência: {', '.join(df_meses_match['YYYYMM_com_match'].tolist())}")


    # ----------------- Resultados -----------------
   st.success(f"Consulta finalizada — últimos {meses} meses (até {ultimo_yyyymm})")

    st.metric("CNPJs encontrados", len(df_cnpjs))

    st.subheader("CNPJs encontrados")
    st.dataframe(df_cnpjs, use_container_width=True)

    # ----------------- Exportação Excel -----------------
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        df_cnpjs.to_excel(writer, index=False, sheet_name="CNPJs")
        df_matches.to_excel(writer, index=False, sheet_name="Arquivos_com_match")
        if not df_errors.empty:
            df_errors.to_excel(writer, index=False, sheet_name="Erros")

    st.download_button(
        label="📥 Baixar Excel",
        data=output.getvalue(),
        file_name=f"resultado_{ativo}_{categoria}_{yyyymm}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # ----------------- Detalhes -----------------
    with st.expander("Arquivos onde o ativo apareceu"):
        st.dataframe(df_matches, use_container_width=True)

    if not df_errors.empty:
        with st.expander("Arquivos com erro"):
            st.dataframe(df_errors, use_container_width=True)
