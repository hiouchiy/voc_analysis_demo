# Databricks notebook source
# MAGIC %md
# MAGIC # 初級①：データ探索（EDA）と可視化
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC
# MAGIC このノートブックでは、アンケートデータを使って「データ探索（EDA: Exploratory Data Analysis）」を行います。
# MAGIC EDAとは、データの中身を見て、どんな傾向があるのかを理解するための基本的な分析手法です。
# MAGIC
# MAGIC ### 具体的に行うこと
# MAGIC 1. **属性別の満足度分析**：年代や性別ごとに、満足度がどう違うかを見ます
# MAGIC 2. **ネガティブコメントの分析**：どんな不満が多いのかを確認します
# MAGIC 3. **地域別の比較**：地域によって不満の内容に違いがあるかを調べます
# MAGIC
# MAGIC ### なぜこれが大切？
# MAGIC データ分析では、いきなり複雑な分析をするのではなく、まずデータの全体像を把握することが重要です。
# MAGIC この作業を通じて、次のステップ（予測モデルの構築など）に進むための土台を作ります。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ1：分析環境の準備
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC データベースとテーブルを指定して、分析の準備をします。
# MAGIC
# MAGIC ### 用語解説
# MAGIC - **CATALOG（カタログ）**：データベースをグループ化する最上位の単位です
# MAGIC - **SCHEMA（スキーマ）**：関連するテーブルをまとめる単位です（データベースとも呼ばれます）
# MAGIC - **GOLD テーブル**：クレンジング（整形）済みの、分析に使える状態のデータです

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
# MAGIC ## ステップ2：データの読み込みと確認
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC GOLDテーブルからデータを読み込み、中身を確認します。
# MAGIC
# MAGIC ### 確認する内容
# MAGIC 1. **データの中身**：最初の10行を表示して、実際のデータがどんな形をしているか見ます
# MAGIC 2. **データの構造**：どんな列（カラム）があって、それぞれどんなデータ型かを確認します
# MAGIC
# MAGIC ### 初学者向けポイント
# MAGIC - `spark.table()` でテーブルをDataFrame（表形式のデータ）として読み込みます
# MAGIC - `display()` でデータを見やすく表示します
# MAGIC - `printSchema()` でデータの構造を確認します

# COMMAND ----------

# GOLDテーブルをDataFrameとして読み込む
gold_df = spark.table(GOLD_TABLE_NAME)

# データの最初の10行を表示
display(gold_df.limit(10))

# データの構造（列名とデータ型）を表示
gold_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ3：年代と性別ごとの満足度分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 年代（age_group）と性別（gender）の組み合わせごとに、以下を計算します：
# MAGIC 1. **回答数**：その組み合わせの人が何人いるか
# MAGIC 2. **平均満足度**：その組み合わせの人たちの満足度の平均値
# MAGIC
# MAGIC ### なぜこれをやるの？
# MAGIC 「20代男性は満足度が低い」「50代女性は満足度が高い」といった傾向を見つけるためです。
# MAGIC このような傾向がわかれば、どの層に対して改善策を打つべきかが明確になります。
# MAGIC
# MAGIC ### コードの流れ
# MAGIC 1. `groupBy()`：年代と性別でグループ分け
# MAGIC 2. `agg()`：各グループで集計（件数と平均を計算）
# MAGIC 3. `orderBy()`：年代と性別の順に並べ替え

# COMMAND ----------

from pyspark.sql import functions as F

# 年代と性別ごとに集計
satisfaction_by_demographics = (
    gold_df
    .groupBy("age_group", "gender")  # 年代と性別でグループ化
    .agg(
        F.count("*").alias("回答数"),  # 各グループの件数
        F.avg("satisfaction_int").alias("平均満足度")  # 各グループの平均満足度
    )
    .orderBy("age_group", "gender")  # 年代、性別の順で並べ替え
)

# 結果を表示
display(satisfaction_by_demographics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ4：満足度の可視化
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 前のセルで集計したデータを、グラフで見やすく表示します。
# MAGIC
# MAGIC ### 可視化の方法
# MAGIC 1. 下のセルを実行すると、表形式でデータが表示されます
# MAGIC 2. 表の上部にある「＋」ボタンをクリック
# MAGIC 3. 「Visualization」を選択
# MAGIC 4. グラフの種類で「Bar（棒グラフ）」を選択
# MAGIC 5. X軸に「age_group」、Y軸に「平均満足度」を設定
# MAGIC 6. 「gender」でグループ化すると、性別ごとに色分けされます
# MAGIC
# MAGIC ### グラフから読み取れること
# MAGIC - どの年代が満足度が高い/低いか
# MAGIC - 性別による満足度の違い
# MAGIC - 回答数が少ないグループ（信頼性が低い可能性がある）

# COMMAND ----------

# グラフ表示用に再度表示
# ※Databricksでは、display()の結果に対してUIでグラフを作成できます
display(satisfaction_by_demographics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ5：ネガティブコメントの分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC ネガティブなフィードバック（不満や要望）がどのカテゴリに分類されているかを集計します。
# MAGIC
# MAGIC ### なぜこれが重要？
# MAGIC 「価格が高い」「品質が悪い」「サポートが不十分」など、
# MAGIC どの不満が多いかがわかれば、優先的に改善すべき点が明確になります。
# MAGIC
# MAGIC ### コードの流れ
# MAGIC 1. `groupBy()`：ネガティブカテゴリごとにグループ化
# MAGIC 2. `agg()`：各カテゴリの出現回数を集計
# MAGIC 3. `orderBy()`：出現回数が多い順に並べ替え

# COMMAND ----------

# ネガティブカテゴリごとの件数を集計
negative_category_counts = (
    gold_df
    .groupBy("negative_feedback_category")  # カテゴリでグループ化
    .agg(F.count("*").alias("件数"))  # 各カテゴリの出現回数
    .orderBy(F.desc("件数"))  # 件数が多い順に並べ替え
)

# 結果を表示
display(negative_category_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ6：ネガティブカテゴリの可視化
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC ネガティブカテゴリの分布を棒グラフで表示します。
# MAGIC
# MAGIC ### 可視化の手順
# MAGIC 1. 下のセルを実行
# MAGIC 2. 表の上部の「＋」→「Visualization」→「Bar」を選択
# MAGIC 3. X軸に「negative_feedback_category」、Y軸に「件数」を設定
# MAGIC
# MAGIC ### グラフから読み取れること
# MAGIC - 最も多い不満は何か
# MAGIC - 不満の種類の多様性（特定の不満に集中しているか、分散しているか）

# COMMAND ----------

# グラフ表示用
display(negative_category_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ7：地域別のネガティブカテゴリ分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 地域ごとに、ネガティブカテゴリの割合を計算します。
# MAGIC
# MAGIC ### なぜこれをやるの？
# MAGIC 同じ商品・サービスでも、地域によって不満の内容が異なる場合があります。
# MAGIC 例：
# MAGIC - 都市部では「価格が高い」という不満が多い
# MAGIC - 地方では「配送が遅い」という不満が多い
# MAGIC
# MAGIC このような地域差がわかれば、地域ごとに異なる対策を立てられます。
# MAGIC
# MAGIC ### コードの流れ（3ステップ）
# MAGIC 1. **地域×カテゴリごとの件数を集計**
# MAGIC 2. **地域ごとの総件数を集計**
# MAGIC 3. **2つを結合して割合を計算**

# COMMAND ----------

# ステップ1: 地域×カテゴリごとの件数を集計
region_category_counts = (
    gold_df
    .groupBy("region", "negative_feedback_category")
    .agg(F.count("*").alias("カテゴリ件数"))
)

# ステップ2: 地域ごとの総件数を集計
region_total_counts = (
    gold_df
    .groupBy("region")
    .agg(F.count("*").alias("地域総件数"))
)

# ステップ3: 2つを結合して割合を計算
region_category_ratio = (
    region_category_counts
    .join(region_total_counts, on="region")  # 地域をキーに結合
    .withColumn(
        "割合", 
        F.round(F.col("カテゴリ件数") / F.col("地域総件数") * 100, 2)  # パーセント表示（小数点2桁）
    )
    .orderBy("region", F.desc("割合"))  # 地域ごとに割合が高い順に並べ替え
)

# 結果を表示
display(region_category_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ まとめ：この章で学んだこと
# MAGIC
# MAGIC ### 実施した分析
# MAGIC 1. **属性別満足度分析**：年代・性別ごとの満足度傾向を把握しました
# MAGIC 2. **ネガティブカテゴリ分析**：どんな不満が多いかを確認しました
# MAGIC 3. **地域別比較**：地域によって不満の内容に違いがあるかを調べました
# MAGIC
# MAGIC ### 分析結果から得られる知見の例
# MAGIC - 「20代の満足度が低い → 若年層向けの改善策が必要」
# MAGIC - 「価格に関する不満が最多 → 価格戦略の見直しが必要」
# MAGIC - 「A地域では配送の不満が多い → A地域の物流を改善すべき」

# COMMAND ----------

# MAGIC %md
# MAGIC ---
