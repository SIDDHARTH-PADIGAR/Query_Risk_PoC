import pandas as pd
import joblib
import numpy as np
from xgboost import XGBClassifier
from sklearn.model_selection import train_test_split
from sklearn.calibration import CalibratedClassifierCV
from sklearn.metrics import classification_report, confusion_matrix
import shap

FEATURES = [
    "num_tables",
    "num_joins",
    "num_filters",
    "num_subqueries",
    "query_length",
    "has_groupby",
    "has_orderby",
    "estimated_table_size_max",
    "estimated_scan_cost",
    "estimated_memory_pressure",
    "estimated_shuffle_risk",
    "estimated_skew_risk",
    "select_star",
    "select_star_columns_estimate"
]

def load_data(path='synthetic_v3.csv'):
    df = pd.read_csv(path)
    for f in FEATURES:
        if f not in df.columns:
            df[f] = 0
    df = df.dropna(subset=['label'])
    return df

def train_and_save(path='synthetic_v3.csv', out='xgb_query_risk.joblib'):
    df = load_data(path)
    X = df[FEATURES].astype(float)
    y = df['label'].astype(int)

    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, stratify=y, random_state=42)

    model = XGBClassifier(n_estimators=300, max_depth=6, learning_rate=0.08, random_state=42, use_label_encoder=False, eval_metric='mlogloss')
    model.fit(X_train, y_train)

    calib = CalibratedClassifierCV(model, method='isotonic', cv=3)
    calib.fit(X_train, y_train)
    final_model = calib

    preds = final_model.predict(X_test)
    print(classification_report(y_test, preds))
    print("Confusion matrix:")
    print(confusion_matrix(y_test, preds))

    try:
        explainer = shap.TreeExplainer(final_model.estimator if hasattr(final_model, 'estimator') else final_model)
        joblib.dump({'model': final_model, 'features': FEATURES, 'explainer': explainer}, out)
    except Exception:
        joblib.dump({'model': final_model, 'features': FEATURES}, out)

    print("saved model to", out)
    return out

if __name__ == '__main__':
    train_and_save()
