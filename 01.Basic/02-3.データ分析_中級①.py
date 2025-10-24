# Databricks notebook source
# MAGIC %md
# MAGIC # 中級①：購入意向予測モデルの構築（機械学習-教師あり学習）
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC
# MAGIC これまでの初級編では、データの傾向を「見る」ことに焦点を当てました。
# MAGIC 今回は、機械学習を使ってデータから「予測する」ことに挑戦します。
# MAGIC
# MAGIC ### ランタイムバージョン
# MAGIC 必ずEnvironment Version 4以上を使ってください。
# MAGIC
# MAGIC ### 予測するもの
# MAGIC **購入意向（purchase_intent）**：アンケート回答者が実際に商品を購入するかどうか
# MAGIC
# MAGIC ### なぜこれが重要？
# MAGIC 「どんな人が購入しやすいか」がわかれば：
# MAGIC - **マーケティング**：購入しそうな人に広告を集中できる
# MAGIC - **営業**：見込み客の優先順位をつけられる
# MAGIC - **商品開発**：どの要素が購入を後押しするかわかる
# MAGIC
# MAGIC ### 機械学習の基本的な流れ
# MAGIC 1. **データ準備**：予測に使う情報（特徴量）を整える
# MAGIC 2. **データ分割**：訓練用と検証用に分ける
# MAGIC 3. **モデル構築**：予測の仕組み（アルゴリズム）を選ぶ
# MAGIC 4. **学習**：過去のデータからパターンを学ぶ
# MAGIC 5. **予測**：新しいデータで予測してみる
# MAGIC 6. **評価**：予測がどれくらい当たっているか確認する
# MAGIC
# MAGIC ### 初学者へのメッセージ
# MAGIC 機械学習は難しそうに見えますが、基本は「過去のデータから規則性を見つけて、未来を予測する」だけです。
# MAGIC このノートブックでは、一つ一つのステップを丁寧に説明しますので、安心して進めてください。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ1：分析環境の準備
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC これまでと同じく、使用するデータベースとテーブルを指定します。

# COMMAND ----------

# 使用するカタログとスキーマの名前を定義
CATALOG = "handson"
SCHEMA = "survey_analysis"

# 指定したカタログとスキーマを使用する設定
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE {SCHEMA}")

# 分析対象のテーブル名
GOLD_TABLE_NAME = "gold_survey_responses_final"

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ2：必要なライブラリのインポート
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 機械学習に必要なツール（ライブラリ）を読み込みます。
# MAGIC
# MAGIC ### 各ライブラリの役割
# MAGIC
# MAGIC #### データ変換系
# MAGIC - **StringIndexer**：文字列（例：「男性」「女性」）を数字に変換
# MAGIC - **OneHotEncoder**：カテゴリを機械学習用の形式に変換
# MAGIC - **VectorAssembler**：複数の特徴量を一つにまとめる
# MAGIC
# MAGIC #### モデル系
# MAGIC - **RandomForestClassifier**：ランダムフォレスト（予測アルゴリズムの一種）
# MAGIC - **LogisticRegression**：ロジスティック回帰（別の予測アルゴリズム）
# MAGIC
# MAGIC #### 評価系
# MAGIC - **MulticlassClassificationEvaluator**：予測精度を計算するツール
# MAGIC
# MAGIC #### その他
# MAGIC - **Pipeline**：複数の処理をまとめて実行する仕組み
# MAGIC
# MAGIC ### 初学者向けポイント
# MAGIC 今は全部を理解する必要はありません。
# MAGIC 「こういうツールがあるんだな」程度の理解で大丈夫です。
# MAGIC 使いながら徐々に理解していきましょう。

# COMMAND ----------

from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql import functions as F

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ3：データの読み込みと確認
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC GOLDテーブルを読み込み、どんなデータがあるか確認します。
# MAGIC
# MAGIC ### 確認のポイント
# MAGIC - **purchase_intent**：予測したい項目（目的変数）
# MAGIC - **age_group, gender, region**：予測に使う属性情報
# MAGIC - **positive_point_category, negative_feedback_category**：コメント情報
# MAGIC - **satisfaction_int**：満足度（数値）

# COMMAND ----------

# GOLDテーブルをDataFrameとして読み込む
gold_df = spark.table(GOLD_TABLE_NAME)

# データの最初の10行を表示
display(gold_df.limit(10))

# データの構造を表示
gold_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ4：特徴量の準備（カテゴリ変数の変換）
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 機械学習モデルは数字しか理解できないため、文字列データを数字に変換します。
# MAGIC
# MAGIC ### なぜこの変換が必要？
# MAGIC
# MAGIC #### 問題：コンピュータは文字を理解できない
# MAGIC - 「20代」「30代」「40代」→ コンピュータには意味不明
# MAGIC - 「男性」「女性」→ これも意味不明
# MAGIC
# MAGIC #### 解決策：2段階の変換
# MAGIC
# MAGIC **ステップ1：StringIndexer（文字→数字）**
# MAGIC ```
# MAGIC 「20代」→ 0
# MAGIC 「30代」→ 1
# MAGIC 「40代」→ 2
# MAGIC ```
# MAGIC
# MAGIC **ステップ2：OneHotEncoder（数字→ベクトル）**
# MAGIC ```
# MAGIC 0 → [1, 0, 0]  # 20代
# MAGIC 1 → [0, 1, 0]  # 30代
# MAGIC 2 → [0, 0, 1]  # 40代
# MAGIC ```
# MAGIC
# MAGIC ### なぜOneHotEncoderが必要？
# MAGIC 単純に「20代=0、30代=1、40代=2」とすると、
# MAGIC コンピュータが「40代は20代の2倍」と誤解してしまいます。
# MAGIC OneHotEncoderを使うことで、この問題を回避できます。
# MAGIC
# MAGIC ### 変換する列
# MAGIC - age_group（年代）
# MAGIC - gender（性別）
# MAGIC - region（地域）
# MAGIC - positive_point_category（ポジティブカテゴリ）
# MAGIC - negative_feedback_category（ネガティブカテゴリ）

# COMMAND ----------

# 変換するカテゴリ列のリスト
categorical_cols = [
    "age_group",                      # 年代
    "gender",                         # 性別
    "region",                         # 地域
    "positive_point_category",        # ポジティブカテゴリ
    "negative_feedback_category"      # ネガティブカテゴリ
]

# ステップ1：文字列を数字に変換（StringIndexer）
# 各列に対して、"列名_idx"という新しい列を作成
# handleInvalid="keep"：未知の値が来ても エラーにしない
# indexers = [
#     StringIndexer(
#         inputCol=col,           # 元の列
#         outputCol=col + "_idx", # 変換後の列名
#         handleInvalid="keep"    # 未知の値を許容
#     ) 
#     for col in categorical_cols
# ]
indexers = []
for col in categorical_cols:
    indexer = StringIndexer()
    indexer.setInputCol(col)
    indexer.setOutputCol(col + "_idx")
    indexer.setHandleInvalid("keep")
    indexers.append(indexer)

# ステップ2：数字をベクトルに変換（OneHotEncoder）
# "列名_idx"を"列名_vec"に変換
# encoders = [
#     OneHotEncoder(
#         inputCol=col + "_idx",  # StringIndexerの出力
#         outputCol=col + "_vec"  # 最終的なベクトル形式
#     ) 
#     for col in categorical_cols
# ]
encoders = []
for col in categorical_cols:
    encoder = OneHotEncoder()
    encoder.setInputCol(col + "_idx")
    encoder.setOutputCol(col + "_vec")
    encoders.append(encoder)

# 数値列（すでに数字なのでそのまま使える）
numeric_cols = ["satisfaction_int"]  # 満足度

# 最終的に使う特徴量の列名リスト
feature_cols = [col + "_vec" for col in categorical_cols] + numeric_cols

print("変換後の特徴量列:")
for col in feature_cols:
    print(f"  - {col}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ5：特徴量の統合
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC バラバラの特徴量を一つの「features」列にまとめます。
# MAGIC
# MAGIC ### なぜまとめる必要がある？
# MAGIC 機械学習モデルは、複数の特徴量を一つのベクトル（数字の配列）として受け取ります。
# MAGIC
# MAGIC ### イメージ
# MAGIC **変換前（バラバラ）**：
# MAGIC ```
# MAGIC age_group_vec: [1, 0, 0]
# MAGIC gender_vec: [0, 1]
# MAGIC satisfaction_int: 4
# MAGIC ```
# MAGIC
# MAGIC **変換後（一つにまとめる）**：
# MAGIC ```
# MAGIC features: [1, 0, 0, 0, 1, 4]
# MAGIC ```
# MAGIC
# MAGIC ### VectorAssemblerの役割
# MAGIC 複数の列を一つのベクトル列に結合するツールです。

# COMMAND ----------

# VectorAssembler：複数の特徴量を一つにまとめる
assembler = VectorAssembler(
    inputCols=feature_cols,      # まとめる列のリスト
    outputCol="features",        # 出力列名
    handleInvalid="keep"         # 欠損値などがあってもエラーにしない
)

print("特徴量の統合設定完了")
print(f"入力列数: {len(feature_cols)}")
print(f"出力列名: features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ6：目的変数の準備
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 予測したい項目（purchase_intent）を数字に変換します。
# MAGIC
# MAGIC ### 目的変数とは？
# MAGIC 機械学習で予測したい対象のことです。
# MAGIC - **特徴量（features）**：予測に使う情報（年代、性別など）
# MAGIC - **目的変数（label）**：予測したいもの（購入意向）
# MAGIC
# MAGIC ### 変換の例
# MAGIC ```
# MAGIC "購入する"     → 0
# MAGIC "購入しない"   → 1
# MAGIC "わからない"   → 2
# MAGIC ```
# MAGIC
# MAGIC ### labelという列名にする理由
# MAGIC SparkMLでは、目的変数の列名を「label」とするのが慣例です。

# COMMAND ----------

# 目的変数を数値に変換
label_indexer = StringIndexer(
    inputCol="purchase_intent",  # 元の列（購入意向）
    outputCol="label",           # 変換後の列名（機械学習の標準名）
    handleInvalid="keep"         # 未知の値を許容
)

print("目的変数の変換設定完了")
print("purchase_intent → label")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ7：データの分割
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC データを「訓練用」と「検証用」に分けます。
# MAGIC
# MAGIC ### なぜ分ける必要がある？
# MAGIC
# MAGIC #### 問題：全データで学習すると...
# MAGIC 学習に使ったデータで評価すると、成績が良く見えすぎてしまいます。
# MAGIC これを「過学習」といいます。
# MAGIC
# MAGIC #### 例え：試験勉強
# MAGIC - **悪い例**：過去問と全く同じ問題が本番で出る
# MAGIC   → 高得点でも実力があるとは限らない
# MAGIC - **良い例**：過去問で勉強して、別の問題で実力を測る
# MAGIC   → 本当の実力がわかる
# MAGIC
# MAGIC ### 分割の比率
# MAGIC - **訓練データ（80%）**：モデルの学習に使う
# MAGIC - **検証データ（20%）**：モデルの性能評価に使う
# MAGIC
# MAGIC ### seedとは？
# MAGIC ランダムに分割する際の「種」です。
# MAGIC 同じseedを使えば、何度実行しても同じ分割結果になります。
# MAGIC （再現性の確保）

# COMMAND ----------

# データを訓練用と検証用に分割（8:2の比率）
train_df, test_df = gold_df.randomSplit(
    [0.8, 0.2],  # 80%を訓練用、20%を検証用
    seed=43      # 再現性のための乱数シード
)

print("データ分割完了")
print(f"訓練データ件数: {train_df.count():,}件")
print(f"検証データ件数: {test_df.count():,}件")
print(f"分割比率: {train_df.count() / gold_df.count():.1%} / {test_df.count() / gold_df.count():.1%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ8：モデルの選択とパイプラインの構築
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 予測アルゴリズムを選び、全処理をまとめた「パイプライン」を作ります。
# MAGIC
# MAGIC ### 予測アルゴリズム：ランダムフォレストとは？
# MAGIC
# MAGIC #### 決定木のイメージ
# MAGIC ```
# MAGIC 年代は30代以上？
# MAGIC   ├─ Yes → 満足度は4以上？
# MAGIC   │         ├─ Yes → 購入する（確率80%）
# MAGIC   │         └─ No  → 購入しない（確率70%）
# MAGIC   └─ No  → 地域は都市部？
# MAGIC             ├─ Yes → 購入する（確率60%）
# MAGIC             └─ No  → 購入しない（確率75%）
# MAGIC ```
# MAGIC
# MAGIC #### ランダムフォレスト
# MAGIC このような決定木を**たくさん作って、多数決で予測**する方法です。
# MAGIC - 1本の木より精度が高い
# MAGIC - 過学習しにくい
# MAGIC - 初心者にも扱いやすい
# MAGIC
# MAGIC ### パイプラインとは？
# MAGIC 複数の処理を順番に実行する仕組みです。
# MAGIC
# MAGIC #### パイプラインの流れ
# MAGIC 1. StringIndexer（文字→数字）
# MAGIC 2. OneHotEncoder（数字→ベクトル）
# MAGIC 3. VectorAssembler（特徴量を統合）
# MAGIC 4. StringIndexer（目的変数を変換）
# MAGIC 5. RandomForest（予測モデル）
# MAGIC
# MAGIC ### パイプラインの利点
# MAGIC - コードがシンプルになる
# MAGIC - 処理の順序を間違えない
# MAGIC - 新しいデータにも同じ処理を簡単に適用できる

# COMMAND ----------

# ランダムフォレストモデルの設定
rf_model = RandomForestClassifier(
    featuresCol="features",  # 特徴量の列名
    labelCol="label",        # 目的変数の列名
    numTrees=50,             # 決定木の数（多いほど精度が上がるが時間もかかる）
    seed=42                  # 再現性のための乱数シード
)

# パイプラインの構築
# 全ての処理をまとめて一つの流れにする
pipeline = Pipeline(stages=[
    *indexers,      # StringIndexer（複数）
    *encoders,      # OneHotEncoder（複数）
    assembler,      # VectorAssembler（特徴量の統合）
    label_indexer,  # 目的変数の変換
    rf_model        # ランダムフォレストモデル
])

print("パイプライン構築完了")
print(f"処理ステージ数: {len(pipeline.getStages())}")
print("処理の流れ:")
print("  1. カテゴリ変数を数値化（StringIndexer）")
print("  2. 数値をベクトル化（OneHotEncoder）")
print("  3. 特徴量を統合（VectorAssembler）")
print("  4. 目的変数を数値化（StringIndexer）")
print("  5. モデルで予測（RandomForest）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ9：モデルの学習と予測
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 1. **学習**：訓練データからパターンを学ぶ
# MAGIC 2. **予測**：検証データで予測を行う
# MAGIC
# MAGIC ### 学習とは？
# MAGIC モデルが訓練データを分析して、「どんな特徴を持つ人が購入するか」の規則性を見つける作業です。
# MAGIC
# MAGIC #### 学習のイメージ
# MAGIC ```
# MAGIC 訓練データを見て...
# MAGIC - 30代で満足度4以上の人は80%が購入している
# MAGIC - 20代で価格に不満がある人は70%が購入しない
# MAGIC - 女性で品質を評価している人は90%が購入する
# MAGIC ...などのパターンを発見
# MAGIC ```
# MAGIC
# MAGIC ### 予測とは？
# MAGIC 学習したパターンを使って、新しいデータの結果を推測することです。
# MAGIC
# MAGIC ### 実行時間について
# MAGIC このセルは数分かかる場合があります。
# MAGIC データ量やモデルの複雑さによって変わります。

# COMMAND ----------

print("モデルの学習を開始します...")
print("（数分かかる場合があります）")

# パイプライン全体を訓練データで学習
model = pipeline.fit(train_df)

print("✓ 学習完了")
print("\n検証データで予測を実行します...")

# 学習済みモデルで検証データを予測
predictions_df = model.transform(test_df)

print("✓ 予測完了")
print("\n予測結果の一部を表示:")

# 予測結果を表示（重要な列のみ）
display(
    predictions_df.select(
        "purchase_intent",  # 実際の購入意向（正解）
        "label",           # 正解を数値化したもの
        "prediction",      # モデルの予測（数値）
        "probability"      # 各クラスの確率
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ10：予測結果の詳細確認
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 予測結果をもっとわかりやすく表示します。
# MAGIC
# MAGIC ### 表示する情報
# MAGIC - **purchase_intent**：実際の購入意向（正解）
# MAGIC - **prediction**：モデルの予測
# MAGIC - **correct**：予測が当たったかどうか（◯/✕）
# MAGIC - **probability**：予測の確信度
# MAGIC
# MAGIC ### probabilityの見方
# MAGIC 例：`[0.2, 0.7, 0.1]`
# MAGIC - クラス0（購入する）の確率：20%
# MAGIC - クラス1（購入しない）の確率：70% ← 最も高いので、これを予測
# MAGIC - クラス2（わからない）の確率：10%

# COMMAND ----------

# 予測が正解かどうかを判定する列を追加
predictions_with_result = predictions_df.withColumn(
    "予測結果",
    F.when(F.col("label") == F.col("prediction"), "◯ 正解").otherwise("✕ 不正解")
)

# わかりやすく表示
display(
    predictions_with_result.select(
        "purchase_intent",     # 実際の値
        "prediction",         # 予測値
        "予測結果",            # 正解/不正解
        "probability",        # 確率
        "age_group",          # 参考：年代
        "gender",             # 参考：性別
        "satisfaction_int"    # 参考：満足度
    ).limit(30)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ11：モデルの性能評価
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC モデルの予測精度を数値で評価します。
# MAGIC
# MAGIC ### 評価指標の説明
# MAGIC
# MAGIC #### 1. Accuracy（正解率）
# MAGIC **意味**：全体のうち、何%を正しく予測できたか
# MAGIC ```
# MAGIC 正解率 = 正しく予測した数 ÷ 全体の数
# MAGIC ```
# MAGIC **例**：100人中80人を正しく予測 → 80%
# MAGIC
# MAGIC **目安**：
# MAGIC - 70%以上：まあまあ良い
# MAGIC - 80%以上：良い
# MAGIC - 90%以上：非常に良い
# MAGIC
# MAGIC #### 2. Precision（適合率）
# MAGIC **意味**：「購入する」と予測した人のうち、実際に購入した人の割合
# MAGIC ```
# MAGIC 適合率 = 正しく「購入」と予測 ÷ 「購入」と予測した全数
# MAGIC ```
# MAGIC **重要な場面**：無駄な広告費を減らしたい時
# MAGIC
# MAGIC #### 3. Recall（再現率）
# MAGIC **意味**：実際に購入した人のうち、何%を「購入する」と予測できたか
# MAGIC ```
# MAGIC 再現率 = 正しく「購入」と予測 ÷ 実際に購入した全数
# MAGIC ```
# MAGIC **重要な場面**：購入者を見逃したくない時
# MAGIC
# MAGIC #### 4. F1スコア
# MAGIC **意味**：PrecisionとRecallの調和平均（バランスを取った指標）
# MAGIC ```
# MAGIC F1 = 2 × (Precision × Recall) ÷ (Precision + Recall)
# MAGIC ```
# MAGIC **特徴**：総合的な性能を表す
# MAGIC
# MAGIC ### 初学者向けポイント
# MAGIC 最初は「Accuracy（正解率）」だけ見れば十分です。
# MAGIC 慣れてきたら、他の指標も見てみましょう。

# COMMAND ----------

# 評価用のツールを準備
evaluator = MulticlassClassificationEvaluator(
    labelCol="label",           # 正解の列
    predictionCol="prediction"  # 予測の列
)

print("=" * 60)
print("モデルの性能評価結果")
print("=" * 60)

# 各評価指標を計算して表示
metrics = {
    "accuracy": "正解率（Accuracy）",
    "f1": "F1スコア",
    "weightedPrecision": "適合率（Precision）",
    "weightedRecall": "再現率（Recall）"
}

results = {}
for metric_name, metric_label in metrics.items():
    score = evaluator.setMetricName(metric_name).evaluate(predictions_df)
    results[metric_name] = score
    print(f"{metric_label:20s}: {score:.4f} ({score*100:.2f}%)")

print("=" * 60)

# 結果の解釈を表示
print("\n📊 結果の解釈:")
accuracy = results["accuracy"]
if accuracy >= 0.9:
    print("✓ 非常に高い精度です！実用レベルのモデルです。")
elif accuracy >= 0.8:
    print("✓ 良い精度です。実務でも使えるレベルです。")
elif accuracy >= 0.7:
    print("△ まあまあの精度です。改善の余地があります。")
else:
    print("✗ 精度が低いです。特徴量の見直しが必要です。")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ12：混同行列（Confusion Matrix）の確認
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 予測の詳細な内訳を「混同行列」で確認します。
# MAGIC
# MAGIC ### 混同行列とは？
# MAGIC 予測と実際の結果を表にまとめたものです。
# MAGIC
# MAGIC #### 例（2クラスの場合）
# MAGIC ```
# MAGIC                予測：購入する  予測：購入しない
# MAGIC 実際：購入する      80              20
# MAGIC 実際：購入しない    10              90
# MAGIC ```
# MAGIC
# MAGIC この表から：
# MAGIC - 正しく「購入する」と予測：80件
# MAGIC - 正しく「購入しない」と予測：90件
# MAGIC - 誤って「購入する」と予測：10件（本当は購入しない）
# MAGIC - 誤って「購入しない」と予測：20件（本当は購入する）
# MAGIC
# MAGIC ### なぜ重要？
# MAGIC - どのクラスの予測が苦手かわかる
# MAGIC - どんな間違いをしているかわかる
# MAGIC - 改善のヒントが得られる

# COMMAND ----------

# 混同行列を作成
confusion_matrix = (
    predictions_df
    .groupBy("label", "prediction")
    .count()
    .orderBy("label", "prediction")
)

print("混同行列（Confusion Matrix）:")
print("=" * 60)
display(confusion_matrix)

print("\n読み方:")
print("- label: 実際の値（正解）")
print("- prediction: モデルの予測")
print("- count: その組み合わせの件数")
print("\n対角線上（label = prediction）が多いほど良いモデルです")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ13：特徴量の重要度分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC どの特徴量が予測に重要だったかを確認します。
# MAGIC
# MAGIC ### 特徴量の重要度とは？
# MAGIC モデルが予測する際に、各特徴量をどれくらい重視したかを数値化したものです。
# MAGIC
# MAGIC #### 例
# MAGIC ```
# MAGIC 満足度：0.35（35%）← 最も重要
# MAGIC 年代：0.25（25%）
# MAGIC 性別：0.15（15%）
# MAGIC 地域：0.10（10%）
# MAGIC その他：0.15（15%）
# MAGIC ```
# MAGIC
# MAGIC ### なぜ重要？
# MAGIC - **施策の優先順位**：重要な要素から改善する
# MAGIC - **モデルの理解**：なぜその予測になったかわかる
# MAGIC - **特徴量の選定**：重要度が低い特徴量は削除できる
# MAGIC
# MAGIC ### 注意点
# MAGIC ランダムフォレストは特徴量の重要度を計算できますが、
# MAGIC 他のアルゴリズム（ロジスティック回帰など）では方法が異なります。

# COMMAND ----------

# モデルから特徴量の重要度を取得
# パイプラインの最後のステージ（RandomForest）を取り出す
rf_model_fitted = model.stages[-1]

# 特徴量の重要度を取得
feature_importances = rf_model_fitted.featureImportances

print("特徴量の重要度:")
print("=" * 60)

# 特徴量名と重要度を対応付けて表示
# （注：OneHotEncodingにより特徴量が増えているため、解釈には注意が必要）
for idx, importance in enumerate(feature_importances):
    if importance > 0.01:  # 重要度が1%以上のものだけ表示
        print(f"特徴量 {idx}: {importance:.4f} ({importance*100:.2f}%)")

print("=" * 60)
print("\n注意:")
print("- 数値が大きいほど重要な特徴量です")
print("- OneHotEncodingにより特徴量が細分化されています")
print("- 同じカテゴリ（例：年代）の重要度を合計すると、")
print("  そのカテゴリ全体の重要度がわかります")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ14：新しいデータでの予測例
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 学習済みモデルを使って、新しいデータの購入意向を予測してみます。
# MAGIC
# MAGIC ### 実務での使い方
# MAGIC 1. **新規顧客の予測**：新しいアンケート回答から購入意向を予測
# MAGIC 2. **ターゲティング**：購入確率が高い人にアプローチ
# MAGIC 3. **シミュレーション**：「満足度を上げたら購入率はどう変わる？」
# MAGIC
# MAGIC ### このセルの流れ
# MAGIC 1. 新しいデータを作成（例として）
# MAGIC 2. モデルで予測
# MAGIC 3. 結果を表示

# COMMAND ----------

# 新しいデータの例を作成
# 実務では、実際の新規アンケートデータを使います
new_data = spark.createDataFrame([
    {
        "age_group": "30代",
        "gender": "女性",
        "region": "関東",
        "satisfaction_int": 5,
        "positive_point_category": "品質",
        "negative_feedback_category": "なし"
    },
    {
        "age_group": "20代",
        "gender": "男性",
        "region": "関西",
        "satisfaction_int": 2,
        "positive_point_category": "なし",
        "negative_feedback_category": "価格"
    },
    {
        "age_group": "40代",
        "gender": "女性",
        "region": "中部",
        "satisfaction_int": 4,
        "positive_point_category": "サポート",
        "negative_feedback_category": "機能"
    }
])

print("新しいデータで予測を実行します...")

# 学習済みモデルで予測
new_predictions = model.transform(new_data)

# 結果を表示
print("\n予測結果:")
print("=" * 60)
display(
    new_predictions.select(
        "age_group",
        "gender",
        "satisfaction_int",
        "prediction",
        "probability"
    )
)

print("\n解釈:")
print("- prediction: 予測されたクラス（数値）")
print("- probability: 各クラスの確率")
print("  例：[0.2, 0.7, 0.1] → クラス1（70%）と予測")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ まとめ：この章で学んだこと
# MAGIC
# MAGIC ### 実施した内容
# MAGIC 1. **データ準備**：カテゴリ変数を数値化し、特徴量を整えました
# MAGIC 2. **モデル構築**：ランダムフォレストで予測モデルを作りました
# MAGIC 3. **学習と予測**：訓練データで学習し、検証データで予測しました
# MAGIC 4. **性能評価**：精度や F1スコアでモデルの性能を確認しました
# MAGIC 5. **実用化**：新しいデータで予測する方法を学びました

# COMMAND ----------

# MAGIC %md
# MAGIC ---
