# Databricks notebook source
# MAGIC %md
# MAGIC # 中級②：クラスタリングによる顧客セグメント分析（機械学習-教師なし学習）
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC
# MAGIC 前回は「購入意向を予測する」教師あり学習を行いました。
# MAGIC 今回は、正解ラベルを使わずにデータのパターンを見つける**教師なし学習**に挑戦します。
# MAGIC
# MAGIC ### ランタイムバージョン
# MAGIC 必ずEnvironment Version 4以上を使ってください。
# MAGIC
# MAGIC ### クラスタリングとは？
# MAGIC 似た特徴を持つデータを自動的にグループ分けする手法です。
# MAGIC
# MAGIC #### 例え：学校のクラス分け
# MAGIC - 先生が決める（教師あり）：「成績順に3クラスに分ける」
# MAGIC - 自然にできる（教師なし）：「気が合う人同士で自然とグループができる」
# MAGIC
# MAGIC クラスタリングは後者のイメージです。
# MAGIC
# MAGIC ### この分析で実現すること
# MAGIC 1. **顧客セグメントの発見**：似た特徴を持つ顧客グループを自動抽出
# MAGIC 2. **各セグメントの特徴把握**：年代構成、満足度、不満の傾向など
# MAGIC 3. **マーケティング施策の立案**：セグメントごとに異なるアプローチ
# MAGIC
# MAGIC ### 使用する手法：K-Means
# MAGIC K-Meansは最も基本的で使いやすいクラスタリング手法です。
# MAGIC
# MAGIC #### K-Meansの仕組み（簡単に）
# MAGIC 1. クラスタの数（K）を決める（例：4グループ）
# MAGIC 2. データを4つのグループに分ける
# MAGIC 3. 各グループ内のデータが似るように調整
# MAGIC 4. これ以上改善できなくなるまで繰り返す
# MAGIC
# MAGIC ### 実務での活用例
# MAGIC - **マーケティング**：セグメントごとに異なる広告を配信
# MAGIC - **商品開発**：各セグメントのニーズに合わせた商品企画
# MAGIC - **カスタマーサポート**：セグメントごとに適したサポート体制

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ1：分析環境の準備
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 使用するデータベースとテーブルを指定します。

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
# MAGIC クラスタリングに必要なツールを読み込みます。
# MAGIC
# MAGIC ### 各ライブラリの役割
# MAGIC - **KMeans**：K-Meansクラスタリングのアルゴリズム
# MAGIC - **StringIndexer, OneHotEncoder, VectorAssembler**：データの前処理（前回と同じ）
# MAGIC
# MAGIC ### 前回との違い
# MAGIC 前回は「分類器（Classifier）」を使いましたが、
# MAGIC 今回は「クラスタリング（Clustering）」を使います。

# COMMAND ----------

from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql import functions as F

print("ライブラリのインポート完了")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ3：データの読み込みと確認
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC GOLDテーブルを読み込み、データの中身を確認します。

# COMMAND ----------

# GOLDテーブルをDataFrameとして読み込む
gold_df = spark.table(GOLD_TABLE_NAME)

# データの最初の10行を表示
display(gold_df.limit(10))

# データの構造を表示
gold_df.printSchema()

print(f"\n総データ件数: {gold_df.count():,}件")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ4：クラスタリングに使う特徴量の選定
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC クラスタリングに使う列を選び、前処理の準備をします。
# MAGIC
# MAGIC ### 選定する特徴量
# MAGIC - **属性情報**：年代、性別、地域
# MAGIC - **感情情報**：ポジティブカテゴリ、ネガティブカテゴリ
# MAGIC - **満足度**：satisfaction_int
# MAGIC
# MAGIC ### 前回（予測モデル）との違い
# MAGIC - **前回**：purchase_intent（購入意向）を予測するために特徴量を使った
# MAGIC - **今回**：purchase_intentは使わず、特徴量だけでグループ分け
# MAGIC
# MAGIC ### データ変換の流れ（前回の復習）
# MAGIC 1. StringIndexer：文字列 → 数字
# MAGIC 2. OneHotEncoder：数字 → ベクトル
# MAGIC 3. VectorAssembler：すべてを一つにまとめる

# COMMAND ----------

# クラスタリングに使うカテゴリ列
categorical_cols = [
    "age_group",                      # 年代
    "gender",                         # 性別
    "region",                         # 地域
    "positive_point_category",        # ポジティブカテゴリ
    "negative_feedback_category"      # ネガティブカテゴリ
]

# クラスタリングに使う数値列
numeric_cols = ["satisfaction_int"]  # 満足度

print("クラスタリングに使う特徴量:")
print("\nカテゴリ変数:")
for col in categorical_cols:
    print(f"  - {col}")
print("\n数値変数:")
for col in numeric_cols:
    print(f"  - {col}")

# StringIndexer：文字列を数字に変換
indexers = [
    StringIndexer(
        inputCol=col,
        outputCol=col + "_idx",
        handleInvalid="keep"
    )
    for col in categorical_cols
]

# OneHotEncoder：数字をベクトルに変換
encoders = [
    OneHotEncoder(
        inputCol=col + "_idx",
        outputCol=col + "_vec"
    )
    for col in categorical_cols
]

# 最終的な特徴量列のリスト
feature_cols = [col + "_vec" for col in categorical_cols] + numeric_cols

print(f"\n変換後の特徴量数: {len(feature_cols)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ5：特徴量の統合
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC バラバラの特徴量を一つの「features」列にまとめます。
# MAGIC
# MAGIC ### 前回との共通点
# MAGIC この処理は前回の予測モデルと全く同じです。
# MAGIC 機械学習では、教師あり・教師なしに関わらず、
# MAGIC 特徴量を一つのベクトルにまとめる必要があります。

# COMMAND ----------

# VectorAssembler：複数の特徴量を一つにまとめる
assembler = VectorAssembler(
    inputCols=feature_cols,
    outputCol="features",
    handleInvalid="keep"
)

print("特徴量の統合設定完了")
print(f"入力列数: {len(feature_cols)}")
print(f"出力列名: features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ6：K-Meansモデルの設定
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC クラスタリングのアルゴリズム（K-Means）を設定します。
# MAGIC
# MAGIC ### クラスタ数（k）の決め方
# MAGIC
# MAGIC #### 初めての場合
# MAGIC まずは**3〜5個**から試すのが一般的です。
# MAGIC
# MAGIC #### クラスタ数の目安
# MAGIC - **少なすぎる（k=2）**：グループが大雑把すぎて使いにくい
# MAGIC - **多すぎる（k=10）**：グループが細かすぎて管理できない
# MAGIC - **適切（k=3〜5）**：マーケティング施策を立てやすい
# MAGIC
# MAGIC #### 最適な数の見つけ方（後述）
# MAGIC - Elbow法（エルボー法）
# MAGIC - シルエット分析
# MAGIC
# MAGIC ### このノートブックでは
# MAGIC まず**k=4**で試してみます。
# MAGIC
# MAGIC ### パラメータの説明
# MAGIC - **k**：クラスタの数
# MAGIC - **featuresCol**：特徴量の列名
# MAGIC - **predictionCol**：クラスタ番号を格納する列名
# MAGIC - **seed**：再現性のための乱数シード

# COMMAND ----------

# クラスタ数を設定
NUM_CLUSTERS = 4

# K-Meansモデルの設定
kmeans_model = KMeans(
    featuresCol="features",      # 特徴量の列名
    predictionCol="cluster",     # クラスタ番号を格納する列名
    k=NUM_CLUSTERS,              # クラスタ数
    seed=42                      # 再現性のための乱数シード
)

print(f"K-Meansモデル設定完了")
print(f"クラスタ数: {NUM_CLUSTERS}")
print(f"出力列名: cluster")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ7：パイプラインの構築
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 前処理とクラスタリングをまとめたパイプラインを作ります。
# MAGIC
# MAGIC ### パイプラインの流れ
# MAGIC 1. StringIndexer（文字→数字）
# MAGIC 2. OneHotEncoder（数字→ベクトル）
# MAGIC 3. VectorAssembler（特徴量を統合）
# MAGIC 4. K-Means（クラスタリング）
# MAGIC
# MAGIC ### 前回（予測モデル）との違い
# MAGIC - **前回**：最後が分類器（RandomForest）
# MAGIC - **今回**：最後がクラスタリング（K-Means）

# COMMAND ----------

# パイプラインの構築
pipeline = Pipeline(stages=[
    *indexers,      # StringIndexer（複数）
    *encoders,      # OneHotEncoder（複数）
    assembler,      # VectorAssembler
    kmeans_model    # K-Means
])

print("パイプライン構築完了")
print(f"処理ステージ数: {len(pipeline.getStages())}")
print("\n処理の流れ:")
print("  1. カテゴリ変数を数値化（StringIndexer）")
print("  2. 数値をベクトル化（OneHotEncoder）")
print("  3. 特徴量を統合（VectorAssembler）")
print("  4. クラスタリング実行（K-Means）")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ8：クラスタリングの実行
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 1. **学習**：データからクラスタの中心を見つける
# MAGIC 2. **割り当て**：各データをどのクラスタに属するか決める
# MAGIC
# MAGIC ### 教師あり学習との違い
# MAGIC
# MAGIC | 項目 | 教師あり学習（前回） | 教師なし学習（今回） |
# MAGIC |------|---------------------|---------------------|
# MAGIC | 正解データ | 必要（purchase_intent） | 不要 |
# MAGIC | データ分割 | 訓練/検証に分ける | 全データを使う |
# MAGIC | 目的 | 正解を予測する | パターンを見つける |
# MAGIC | 評価 | 正解率で評価 | 解釈で評価 |
# MAGIC
# MAGIC ### 実行時間
# MAGIC データ量によって数分かかる場合があります。

# COMMAND ----------

print("クラスタリングを開始します...")
print("（数分かかる場合があります）")

# パイプライン全体を実行（学習とクラスタ割り当て）
clustering_model = pipeline.fit(gold_df)

print("✓ クラスタリング完了")
print("\n各データにクラスタを割り当てます...")

# 各データにクラスタ番号を付与
clustered_df = clustering_model.transform(gold_df)

print("✓ クラスタ割り当て完了")
print("\n結果の一部を表示:")

# 結果を表示
display(
    clustered_df.select(
        "cluster",                      # クラスタ番号
        "age_group",                    # 年代
        "gender",                       # 性別
        "satisfaction_int",             # 満足度
        "negative_feedback_category"    # ネガティブカテゴリ
    ).limit(20)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ9：クラスタの基本統計
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 各クラスタの基本的な情報を確認します。
# MAGIC
# MAGIC ### 確認する内容
# MAGIC 1. **クラスタごとの件数**：各グループに何人いるか
# MAGIC 2. **平均満足度**：各グループの満足度の傾向
# MAGIC
# MAGIC ### 見るべきポイント
# MAGIC - クラスタの大きさは均等か？
# MAGIC - 満足度に差はあるか？
# MAGIC - 極端に小さいクラスタはないか？

# COMMAND ----------

# クラスタごとの基本統計を集計
cluster_summary = (
    clustered_df
    .groupBy("cluster")
    .agg(
        F.count("*").alias("人数"),
        F.avg("satisfaction_int").alias("平均満足度"),
        F.min("satisfaction_int").alias("最小満足度"),
        F.max("satisfaction_int").alias("最大満足度")
    )
    .orderBy("cluster")
)

print("=" * 60)
print("クラスタごとの基本統計")
print("=" * 60)
display(cluster_summary)

# 全体に対する割合を計算
total_count = gold_df.count()
cluster_summary_with_ratio = cluster_summary.withColumn(
    "割合（%）",
    F.round(F.col("人数") / total_count * 100, 2)
)

print("\n割合を含む統計:")
display(cluster_summary_with_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ10：クラスタの中心座標の確認
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 各クラスタの「中心」がどこにあるかを確認します。
# MAGIC
# MAGIC ### クラスタ中心とは？
# MAGIC そのクラスタの「代表的な特徴」を表す座標です。
# MAGIC
# MAGIC #### イメージ
# MAGIC ```
# MAGIC クラスタ0の中心: [0.2, 0.8, 0.1, 4.5, ...]
# MAGIC クラスタ1の中心: [0.7, 0.1, 0.3, 2.3, ...]
# MAGIC ```
# MAGIC
# MAGIC ### 注意点
# MAGIC OneHotEncodingにより特徴量が増えているため、
# MAGIC 数値だけでは解釈が難しいです。
# MAGIC 次のステップで、もっとわかりやすい形で分析します。

# COMMAND ----------

# K-Meansモデルを取り出す（パイプラインの最後のステージ）
kmeans_fitted = clustering_model.stages[-1]

# クラスタ中心を取得
cluster_centers = kmeans_fitted.clusterCenters()

print("=" * 60)
print("クラスタ中心の座標")
print("=" * 60)
for i, center in enumerate(cluster_centers):
    print(f"\nクラスタ {i}:")
    print(f"  座標: {center[:10]}...")  # 最初の10次元のみ表示
    print(f"  次元数: {len(center)}")

print("\n" + "=" * 60)
print("注意: OneHotEncodingにより特徴量が多次元化されています")
print("次のステップで、より解釈しやすい形で分析します")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ11：クラスタごとの属性分布分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 各クラスタの**年代構成**を分析します。
# MAGIC
# MAGIC ### なぜこれが重要？
# MAGIC 「クラスタ0は20代中心」「クラスタ1は50代中心」といった
# MAGIC **わかりやすい特徴**を見つけることができます。
# MAGIC
# MAGIC ### 分析内容
# MAGIC クラスタごとに、各年代の割合を計算します。

# COMMAND ----------

# クラスタ×年代の集計
cluster_age_counts = (
    clustered_df
    .groupBy("cluster", "age_group")
    .agg(F.count("*").alias("人数"))
)

# クラスタごとの総数
cluster_totals = (
    clustered_df
    .groupBy("cluster")
    .agg(F.count("*").alias("クラスタ総数"))
)

# 割合を計算
cluster_age_ratio = (
    cluster_age_counts
    .join(cluster_totals, on="cluster")
    .withColumn(
        "割合（%）",
        F.round(F.col("人数") / F.col("クラスタ総数") * 100, 2)
    )
    .orderBy("cluster", F.desc("割合（%）"))
)

print("=" * 60)
print("クラスタごとの年代構成")
print("=" * 60)
display(cluster_age_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ12：クラスタごとの性別分布分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 各クラスタの**性別構成**を分析します。
# MAGIC
# MAGIC ### 分析のポイント
# MAGIC - 男性中心のクラスタはあるか？
# MAGIC - 女性中心のクラスタはあるか？
# MAGIC - 性別が均等なクラスタはあるか？

# COMMAND ----------

# クラスタ×性別の集計
cluster_gender_counts = (
    clustered_df
    .groupBy("cluster", "gender")
    .agg(F.count("*").alias("人数"))
)

# 割合を計算
cluster_gender_ratio = (
    cluster_gender_counts
    .join(cluster_totals, on="cluster")
    .withColumn(
        "割合（%）",
        F.round(F.col("人数") / F.col("クラスタ総数") * 100, 2)
    )
    .orderBy("cluster", F.desc("割合（%）"))
)

print("=" * 60)
print("クラスタごとの性別構成")
print("=" * 60)
display(cluster_gender_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ13：クラスタごとのネガティブカテゴリ分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 各クラスタの**不満の傾向**を分析します。
# MAGIC
# MAGIC ### なぜこれが最も重要？
# MAGIC クラスタの特徴を最もよく表すのが、この不満の傾向です。
# MAGIC
# MAGIC #### 例
# MAGIC - **クラスタ0**：価格への不満が多い → 価格重視層
# MAGIC - **クラスタ1**：品質への不満が多い → 品質重視層
# MAGIC - **クラスタ2**：サポートへの不満が多い → サポート重視層
# MAGIC
# MAGIC ### マーケティングへの活用
# MAGIC この分析結果から、各クラスタに対する施策を立てられます。

# COMMAND ----------

# クラスタ×ネガティブカテゴリの集計
cluster_negative_counts = (
    clustered_df
    .groupBy("cluster", "negative_feedback_category")
    .agg(F.count("*").alias("人数"))
)

# 割合を計算
cluster_negative_ratio = (
    cluster_negative_counts
    .join(cluster_totals, on="cluster")
    .withColumn(
        "割合（%）",
        F.round(F.col("人数") / F.col("クラスタ総数") * 100, 2)
    )
    .orderBy("cluster", F.desc("割合（%）"))
)

print("=" * 60)
print("クラスタごとのネガティブカテゴリ構成")
print("=" * 60)
print("各クラスタの主な不満を確認できます")
display(cluster_negative_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ14：クラスタごとのポジティブカテゴリ分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 各クラスタの**評価ポイントの傾向**を分析します。
# MAGIC
# MAGIC ### 不満だけでなく強みも知る
# MAGIC ネガティブだけでなく、ポジティブな評価も分析することで、
# MAGIC より立体的にクラスタの特徴を理解できます。

# COMMAND ----------

# クラスタ×ポジティブカテゴリの集計
cluster_positive_counts = (
    clustered_df
    .groupBy("cluster", "positive_point_category")
    .agg(F.count("*").alias("人数"))
)

# 割合を計算
cluster_positive_ratio = (
    cluster_positive_counts
    .join(cluster_totals, on="cluster")
    .withColumn(
        "割合（%）",
        F.round(F.col("人数") / F.col("クラスタ総数") * 100, 2)
    )
    .orderBy("cluster", F.desc("割合（%）"))
)

print("=" * 60)
print("クラスタごとのポジティブカテゴリ構成")
print("=" * 60)
print("各クラスタが何を評価しているかを確認できます")
display(cluster_positive_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ15：クラスタプロファイルの総合表示
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC これまでの分析結果を統合して、各クラスタの特徴を一覧表示します。
# MAGIC
# MAGIC ### クラスタプロファイルとは？
# MAGIC 各クラスタの特徴をまとめた「顔写真」のようなものです。

# COMMAND ----------

print("=" * 80)
print("クラスタプロファイル総合表")
print("=" * 80)

for cluster_id in range(NUM_CLUSTERS):
    print(f"\n{'='*80}")
    print(f"クラスタ {cluster_id} のプロファイル")
    print(f"{'='*80}")
    
    # 基本統計
    cluster_stats = cluster_summary_with_ratio.filter(F.col("cluster") == cluster_id).collect()[0]
    print(f"\n【基本情報】")
    print(f"  人数: {cluster_stats['人数']:,}人 ({cluster_stats['割合（%）']}%)")
    print(f"  平均満足度: {cluster_stats['平均満足度']:.2f}")
    
    # 主な年代（上位2つ）
    top_ages = (
        cluster_age_ratio
        .filter(F.col("cluster") == cluster_id)
        .orderBy(F.desc("割合（%）"))
        .limit(2)
        .collect()
    )
    print(f"\n【主な年代】")
    for age in top_ages:
        print(f"  {age['age_group']}: {age['割合（%）']}%")
    
    # 主な性別
    top_genders = (
        cluster_gender_ratio
        .filter(F.col("cluster") == cluster_id)
        .orderBy(F.desc("割合（%）"))
        .collect()
    )
    print(f"\n【性別構成】")
    for gender in top_genders:
        print(f"  {gender['gender']}: {gender['割合（%）']}%")
    
    # 主なネガティブカテゴリ（上位3つ）
    top_negatives = (
        cluster_negative_ratio
        .filter(F.col("cluster") == cluster_id)
        .orderBy(F.desc("割合（%）"))
        .limit(3)
        .collect()
    )
    print(f"\n【主な不満】")
    for neg in top_negatives:
        print(f"  {neg['negative_feedback_category']}: {neg['割合（%）']}%")
    
    # 主なポジティブカテゴリ（上位3つ）
    top_positives = (
        cluster_positive_ratio
        .filter(F.col("cluster") == cluster_id)
        .orderBy(F.desc("割合（%）"))
        .limit(3)
        .collect()
    )
    print(f"\n【主な評価ポイント】")
    for pos in top_positives:
        print(f"  {pos['positive_point_category']}: {pos['割合（%）']}%")

print(f"\n{'='*80}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ16：新規データのクラスタ予測
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 学習済みモデルを使って、新しいデータがどのクラスタに属するか予測します。
# MAGIC
# MAGIC ### 実務での使い方
# MAGIC 1. **新規顧客の分類**：新しいアンケート回答者を自動分類
# MAGIC 2. **パーソナライズ**：クラスタに応じた対応を即座に実施
# MAGIC 3. **モニタリング**：時間とともにクラスタ構成がどう変化するか追跡

# COMMAND ----------

# 新しいデータの例を作成
new_customers = spark.createDataFrame([
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
        "age_group": "50代",
        "gender": "女性",
        "region": "中部",
        "satisfaction_int": 3,
        "positive_point_category": "サポート",
        "negative_feedback_category": "機能"
    }
])

print("新規データのクラスタ予測を実行します...\n")

# 学習済みモデルでクラスタを予測
new_predictions = clustering_model.transform(new_customers)

# 結果を表示
display(
    new_predictions.select(
        "age_group",
        "gender",
        "satisfaction_int",
        "negative_feedback_category",
        "cluster"
    )
)

print("\n各顧客が割り当てられたクラスタ番号を確認してください")
print("クラスタ番号から、その顧客の特徴を推測できます")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ まとめ：この章で学んだこと
# MAGIC
# MAGIC ### 実施した内容
# MAGIC 1. **クラスタリング実行**：K-Meansで顧客を4グループに自動分類
# MAGIC 2. **クラスタ分析**：各グループの特徴を多角的に分析
# MAGIC 3. **プロファイル作成**：各クラスタの「顔」を明確化
# MAGIC 4. **予測への応用**：新規顧客の自動分類
# MAGIC
# MAGIC ### クラスタリングの価値
# MAGIC
# MAGIC #### 発見した洞察の例
# MAGIC - **クラスタ0**：若年層・価格重視・満足度低め → 価格施策が必要
# MAGIC - **クラスタ1**：中年層・品質重視・満足度高め → ロイヤル顧客
# MAGIC - **クラスタ2**：シニア層・サポート重視 → サポート強化が効果的
# MAGIC - **クラスタ3**：バランス型・満足度中程度 → 潜在的な成長層
# MAGIC
# MAGIC #### マーケティングへの活用
# MAGIC - **セグメント別広告**：各クラスタに最適なメッセージを配信
# MAGIC - **商品開発**：各クラスタのニーズに合わせた商品企画
# MAGIC - **カスタマーサポート**：クラスタに応じたサポート体制
# MAGIC - **リソース配分**：重要なクラスタに経営資源を集中
# MAGIC
# MAGIC ### さらに発展させる方法
# MAGIC
# MAGIC #### 1. 最適なクラスタ数の探索
# MAGIC ```python
# MAGIC # Elbow法：複数のkで試して最適な数を見つける
# MAGIC for k in range(2, 11):
# MAGIC     kmeans = KMeans(k=k, seed=42)
# MAGIC     model = kmeans.fit(features)
# MAGIC     cost = model.summary.trainingCost
# MAGIC     print(f"k={k}: cost={cost}")
# MAGIC ```
# MAGIC
# MAGIC #### 2. シルエット分析
# MAGIC クラスタの品質を定量的に評価する手法
# MAGIC
# MAGIC #### 3. 階層的クラスタリング
# MAGIC K-Meansとは異なるアプローチでクラスタリング
# MAGIC
# MAGIC #### 4. クラスタと購入意向の関係分析
# MAGIC ```python
# MAGIC # クラスタごとの購入意向を分析
# MAGIC cluster_purchase = (
# MAGIC     clustered_df
# MAGIC     .groupBy("cluster", "purchase_intent")
# MAGIC     .count()
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### 5. 時系列でのクラスタ変化追跡
# MAGIC 月ごとにクラスタ構成がどう変化するか分析

# COMMAND ----------

# MAGIC %md
# MAGIC ---
