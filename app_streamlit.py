import streamlit as st
from infer import predict
import pandas as pd
import numpy as np

st.set_page_config(page_title="Query Risk PoC", layout="wide")
st.title("Query Risk Detector — e6data PoC")

sql = st.text_area("SQL Query", height=240, value="SELECT * FROM big_sales_table WHERE amount > 500")

if st.button("Analyze"):
    if not sql.strip():
        st.warning("Paste a SQL query.")
    else:
        with st.spinner("Analyzing..."):
            out = predict(sql)

        pred = out["prediction"]
        probs = out["probabilities"]
        meta = out["metadata"]
        shap_vals = out.get("shap")

        # ---------------- Risk Header ----------------
        col1, col2 = st.columns([1, 2])
        with col1:
            if pred == 0:
                st.success("Low Risk")
            elif pred == 1:
                st.warning("Medium Risk")
            else:
                st.error("High Risk")

            if probs:
                st.write("Probabilities (low, med, high):",
                         [round(x, 3) for x in probs])

        # ---------------- Basic Metrics ----------------
        with col2:
            m1, m2, m3 = st.columns(3)
            m1.metric("Tables", meta.get("num_tables", 0))
            m2.metric("Joins", meta.get("num_joins", 0))
            m3.metric("Max Table Size (rows)", meta.get("estimated_table_size_max", 0))

        # ---------------- Metadata Panel ----------------
        st.subheader("Extracted Metadata")
        st.json(meta)

        # ---------------- SHAP Visualizations ----------------
        st.subheader("Feature Contributions (SHAP)")

        if shap_vals:
            # Convert SHAP dict to dataframe
            df = (
                pd.DataFrame({
                    "feature": list(shap_vals.keys()),
                    "shap": [float(v) for v in shap_vals.values()]
                })
                .sort_values("shap", key=abs, ascending=False)
            )

            # Bar chart of top contributors
            st.write("Top Contributors:")
            st.bar_chart(df.set_index("feature")["shap"].head(10))

            # Raw table (optional)
            with st.expander("Show full SHAP table"):
                st.dataframe(df)
        else:
            st.info("SHAP explanation unavailable for this model.")
        
        # ---------------- Explanation Block ----------------
        st.subheader("Why this prediction?")
        explanation = []

        # High-impact logic (same style as a real production query-control system)
        if meta.get("num_joins", 0) >= 2:
            explanation.append("• Multiple joins increase compute and spill risk.")

        if meta.get("select_star", 0) == 1:
            explanation.append("• SELECT * triggers wide scans, increasing memory pressure.")

        if meta.get("estimated_table_size_max", 0) > 5_000_000:
            explanation.append("• Query touches very large tables.")

        if meta.get("num_subqueries", 0) >= 2:
            explanation.append("• Nested subqueries increase pipeline complexity.")

        if meta.get("has_groupby", 0) == 1:
            explanation.append("• GROUP BY triggers aggregation shuffles.")

        if meta.get("window_functions", 0) == 1:
            explanation.append("• Window functions are memory-heavy operations.")

        if meta.get("udf_usage", 0) == 1:
            explanation.append("• UDF detected — may introduce unpredictable cost.")

        if meta.get("cartesian_join", 0) == 1:
            explanation.append("• Cartesian join detected — catastrophic fan-out risk.")

        if not explanation:
            explanation = ["• Model determined the query characteristics are low-risk."]

        for line in explanation:
            st.write(line)
