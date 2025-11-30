import sys
import joblib
import numpy as np
from metadata_extractor import extract_metadata

MODEL_PATH = "xgb_query_risk.joblib"

def build_feature_vector(meta, feature_names):
    return [meta.get(f, 0) for f in feature_names]

def predict(query, model_path=MODEL_PATH):
    obj = joblib.load(model_path)
    model = obj['model'] if isinstance(obj, dict) and 'model' in obj else obj
    feature_names = obj['features'] if isinstance(obj, dict) and 'features' in obj else [
        "num_tables","num_joins","num_filters","num_subqueries","query_length",
        "has_groupby","has_orderby","estimated_table_size_max","estimated_scan_cost",
        "estimated_memory_pressure","estimated_shuffle_risk","estimated_skew_risk",
        "select_star","select_star_columns_estimate"
    ]

    # Extract SHAP explainer if available
    explainer = obj.get("explainer") if isinstance(obj, dict) else None

    meta = extract_metadata(query)
    X = np.array([build_feature_vector(meta, feature_names)])

    pred = int(model.predict(X)[0])
    proba = model.predict_proba(X)[0].tolist() if hasattr(model, "predict_proba") else None

    # ---------- SHAP CALCULATION (MUST COME BEFORE RULE OVERRIDES) ----------
    shap_values = None
    try:
        if explainer is not None:
            sv = explainer.shap_values(X)
            if isinstance(sv, list):     # multiclass: pick predicted class
                sv = sv[pred]
            shap_values = {
                feature_names[i]: float(sv[0][i])
                for i in range(len(feature_names))
            }
    except Exception:
        shap_values = None
    # ------------------------------------------------------------------------

    # -------- RULE OVERRIDES (HARD GUARDS) --------
    # 1. Cartesian Join → ALWAYS HIGH
    if meta.get("cartesian_join", 0) == 1:
        return {
            "prediction": 2,
            "probabilities": [0.0, 0.0, 1.0],
            "metadata": meta,
            "shap": shap_values
        }

    # 2. Large table + SELECT * → HIGH
    if meta.get("estimated_table_size_max", 0) > 20_000_000 and meta.get("select_star", 0) == 1:
        return {
            "prediction": 2,
            "probabilities": [0.0, 0.1, 0.9],
            "metadata": meta,
            "shap": shap_values
        }

    # ---------------- Final Normal Model Output ----------------
    return {
        "prediction": pred,
        "probabilities": proba,
        "metadata": meta,
        "shap": shap_values
    }

if __name__ == "__main__":
    q = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else ""
    if not q:
        print("Usage: python infer.py \"<SQL query>\"")
        sys.exit(1)
    out = predict(q)
    import json
    print(json.dumps(out, indent=2))
