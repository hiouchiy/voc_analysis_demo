# Databricks notebook source
# MAGIC %md
# MAGIC # 初級②：属性とコメントカテゴリの関係分析
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC
# MAGIC 前回（初級①）では、データ全体の傾向を把握しました。
# MAGIC 今回は、**属性（年代・性別など）とコメントの内容にどんな関係があるか**を深掘りします。
# MAGIC
# MAGIC ### 具体的に行うこと
# MAGIC 1. **年代別の不満分析**：年代によって不満の内容がどう違うかを見ます
# MAGIC 2. **ポジティブコメント分析**：どんな良い点を挙げた人が満足度が高いかを調べます
# MAGIC 3. **属性別のポジ・ネガ比率**：性別などで、ポジティブ・ネガティブコメントの出現率を比較します
# MAGIC
# MAGIC ### なぜこの分析が重要？
# MAGIC 「20代は価格に不満が多い」「女性はサポート面を評価する傾向がある」といった
# MAGIC **属性ごとの特徴**がわかれば、ターゲットを絞った改善策や施策を立てられます。
# MAGIC
# MAGIC ### 前回との違い
# MAGIC - 前回：全体の傾向を把握（「何が多いか」を見る）
# MAGIC - 今回：属性との関係を分析（「誰がどう感じているか」を見る）

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ1：分析環境の準備
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 前回と同じく、使用するデータベースとテーブルを指定します。
# MAGIC
# MAGIC ### 復習：用語の意味
# MAGIC - **CATALOG**：データの最上位グループ
# MAGIC - **SCHEMA**：関連するテーブルをまとめる単位
# MAGIC - **GOLD テーブル**：分析用に整形済みのデータ

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
# MAGIC GOLDテーブルを読み込み、データの中身と構造を確認します。
# MAGIC
# MAGIC ### 確認のポイント
# MAGIC - `positive_point_category`：ポジティブコメントのカテゴリ
# MAGIC - `negative_feedback_category`：ネガティブコメントのカテゴリ
# MAGIC - これらが属性（age_group、genderなど）とどう関連しているかを分析します

# COMMAND ----------

# GOLDテーブルをDataFrameとして読み込む
gold_df = spark.table(GOLD_TABLE_NAME)

# データの最初の10行を表示
display(gold_df.limit(10))

# データの構造（列名とデータ型）を表示
gold_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ3：年代別のネガティブカテゴリ集計
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 年代（age_group）ごとに、各ネガティブカテゴリが何件出現したかを集計します。
# MAGIC
# MAGIC ### なぜこれをやるの？
# MAGIC 「20代は価格の不満が多い」「50代はサポートの不満が多い」といった
# MAGIC **年代による不満の違い**を見つけるためです。
# MAGIC
# MAGIC ### 次のステップへの準備
# MAGIC このセルでは件数を集計し、次のセルで「割合」に変換します。
# MAGIC 割合にすることで、回答数が異なる年代間でも公平に比較できます。

# COMMAND ----------

from pyspark.sql import functions as F

# 年代×ネガティブカテゴリごとの件数を集計
age_negative_counts = (
    gold_df
    .groupBy("age_group", "negative_feedback_category")  # 年代とカテゴリでグループ化
    .agg(F.count("*").alias("件数"))  # 各組み合わせの出現回数
)

# 結果を表示
display(age_negative_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ4：年代別ネガティブカテゴリの割合計算
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 年代ごとのネガティブカテゴリの**割合**を計算します。
# MAGIC
# MAGIC ### なぜ割合が必要？
# MAGIC 例えば：
# MAGIC - 20代の回答者：100人
# MAGIC - 50代の回答者：50人
# MAGIC
# MAGIC この場合、単純な件数では20代の方が多くなってしまいます。
# MAGIC 割合にすることで、「その年代の中で何%がその不満を持っているか」がわかります。
# MAGIC
# MAGIC ### コードの流れ（2ステップ）
# MAGIC 1. **年代ごとの総回答数を集計**
# MAGIC 2. **カテゴリ件数 ÷ 総回答数 で割合を計算**

# COMMAND ----------

# ステップ1: 年代ごとの総回答数を集計
age_total_counts = (
    gold_df
    .groupBy("age_group")
    .agg(F.count("*").alias("年代総数"))
)

# ステップ2: 件数データと総数データを結合して割合を計算
age_negative_ratio = (
    age_negative_counts
    .join(age_total_counts, on="age_group")  # 年代をキーに結合
    .withColumn(
        "割合（%）", 
        F.round(F.col("件数") / F.col("年代総数") * 100, 2)  # パーセント表示
    )
    .orderBy("age_group", F.desc("割合（%）"))  # 年代ごとに割合が高い順
)

# 結果を表示
display(age_negative_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ5：年代別ネガティブカテゴリの可視化
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 年代ごとのネガティブカテゴリ構成を、積み上げ棒グラフで可視化します。
# MAGIC
# MAGIC ### 可視化の手順
# MAGIC 1. 下のセルを実行
# MAGIC 2. 表の上部の「＋」→「Visualization」を選択
# MAGIC 3. グラフの種類で「Bar（棒グラフ）」または「Stacked bar（積み上げ棒グラフ）」を選択
# MAGIC 4. 設定：
# MAGIC    - X軸：`age_group`（年代）
# MAGIC    - Y軸：`割合（%）`
# MAGIC    - グループ化：`negative_feedback_category`（カテゴリごとに色分け）
# MAGIC
# MAGIC ### グラフから読み取れること
# MAGIC - 各年代で最も多い不満カテゴリ
# MAGIC - 年代による不満の構成の違い
# MAGIC - 特定の年代に偏っている不満カテゴリ

# COMMAND ----------

# グラフ表示用
display(age_negative_ratio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ6：ポジティブカテゴリ別の満足度分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC どのポジティブカテゴリを挙げた人が、満足度が高いかを分析します。
# MAGIC
# MAGIC ### なぜこれが重要？
# MAGIC 例えば：
# MAGIC - 「品質」を評価した人の平均満足度：4.5
# MAGIC - 「価格」を評価した人の平均満足度：3.8
# MAGIC
# MAGIC この場合、「品質が満足度に強く影響している」ことがわかります。
# MAGIC つまり、**どの強みを伸ばすべきか**の優先順位がつけられます。
# MAGIC
# MAGIC ### 集計内容
# MAGIC 1. **平均満足度**：そのカテゴリを挙げた人の満足度の平均
# MAGIC 2. **件数**：そのカテゴリを挙げた人の数（信頼性の確認用）

# COMMAND ----------

# ポジティブカテゴリごとに満足度と件数を集計
positive_satisfaction = (
    gold_df
    .groupBy("positive_point_category")  # ポジティブカテゴリでグループ化
    .agg(
        F.avg("satisfaction_int").alias("平均満足度"),  # 平均満足度
        F.count("*").alias("件数")  # 件数
    )
    .orderBy(F.desc("件数"))  # 件数が多い順に並べ替え
)

# 結果を表示
display(positive_satisfaction)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ7：ポジティブカテゴリ別満足度の可視化
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC ポジティブカテゴリごとの平均満足度を棒グラフで表示します。
# MAGIC
# MAGIC ### 可視化の手順
# MAGIC 1. 下のセルを実行
# MAGIC 2. 「＋」→「Visualization」→「Bar」を選択
# MAGIC 3. 設定：
# MAGIC    - X軸：`positive_point_category`
# MAGIC    - Y軸：`平均満足度`
# MAGIC    - （オプション）バブルサイズ：`件数`（件数も同時に表現できます）
# MAGIC
# MAGIC ### グラフから読み取れること
# MAGIC - どのポジティブ要素が満足度に最も貢献しているか
# MAGIC - 件数が少ないカテゴリ（信頼性が低い可能性）
# MAGIC - 伸ばすべき強みの優先順位

# COMMAND ----------

# グラフ表示用
display(positive_satisfaction)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ8：属性別のポジ・ネガ比率分析
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 性別ごとに、以下の比率を計算します：
# MAGIC 1. **ポジティブコメント出現率**：ポジティブコメントを書いた人の割合
# MAGIC 2. **ネガティブコメント出現率**：ネガティブコメントを書いた人の割合
# MAGIC
# MAGIC ### なぜこれが重要？
# MAGIC 例えば：
# MAGIC - 男性：ポジティブ60%、ネガティブ40%
# MAGIC - 女性：ポジティブ70%、ネガティブ30%
# MAGIC
# MAGIC この場合、「女性の方がポジティブな評価をする傾向がある」ことがわかります。
# MAGIC
# MAGIC ### 応用例
# MAGIC - マーケティング：どの属性にアプローチすべきか
# MAGIC - 改善施策：どの属性の不満を優先的に解消すべきか
# MAGIC
# MAGIC ### コードの流れ
# MAGIC 1. 性別ごとの総数を集計
# MAGIC 2. ポジティブコメントがある件数を集計
# MAGIC 3. ネガティブコメントがある件数を集計
# MAGIC 4. それぞれの割合を計算

# COMMAND ----------

# 性別ごとのポジ・ネガ比率を計算
positive_negative_by_gender = (
    gold_df
    .groupBy("gender")  # 性別でグループ化
    .agg(
        # 総数
        F.count("*").alias("総回答数"),
        
        # ポジティブコメントがある件数
        # （positive_pointがnullでなく、空文字でもない場合にカウント）
        F.sum(
            F.when(
                (F.col("positive_point").isNotNull()) & (F.col("positive_point") != ""), 
                1
            ).otherwise(0)
        ).alias("ポジティブ件数"),
        
        # ネガティブコメントがある件数
        F.sum(
            F.when(
                (F.col("negative_feedback").isNotNull()) & (F.col("negative_feedback") != ""), 
                1
            ).otherwise(0)
        ).alias("ネガティブ件数")
    )
    # 割合を計算（パーセント表示）
    .withColumn("ポジティブ率（%）", F.round(F.col("ポジティブ件数") / F.col("総回答数") * 100, 2))
    .withColumn("ネガティブ率（%）", F.round(F.col("ネガティブ件数") / F.col("総回答数") * 100, 2))
    .orderBy("gender")
)

# 結果を表示
display(positive_negative_by_gender)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ まとめ：この章で学んだこと
# MAGIC
# MAGIC ### 実施した分析
# MAGIC 1. **年代別ネガティブ分析**：年代によって不満の内容がどう違うかを可視化しました
# MAGIC 2. **ポジティブ要素と満足度の関係**：どの強みが満足度に貢献しているかを分析しました
# MAGIC 3. **属性別ポジ・ネガ比率**：性別でポジティブ・ネガティブの出現傾向を比較しました
# MAGIC
# MAGIC ### 分析結果から得られる知見の例
# MAGIC
# MAGIC #### 年代別の傾向
# MAGIC - 「20代は価格への不満が30%で最多 → 若年層向け価格プランの検討」
# MAGIC - 「50代はサポートへの不満が25% → シニア向けサポート強化」
# MAGIC
# MAGIC #### ポジティブ要素の影響
# MAGIC - 「品質を評価した人の満足度が4.5で最高 → 品質訴求が効果的」
# MAGIC - 「価格を評価した人は少ないが満足度は高い → 価格メリットの訴求不足」
# MAGIC
# MAGIC #### 属性別の感情傾向
# MAGIC - 「女性の方がポジティブコメント率が高い → 女性向けマーケティングが有効」
# MAGIC - 「男性のネガティブ率が高い → 男性の不満解消が優先課題」
# MAGIC
# MAGIC ### 前回（初級①）との違い
# MAGIC
# MAGIC | 項目 | 初級① | 初級②（今回） |
# MAGIC |------|-------|---------------|
# MAGIC | 分析の焦点 | 全体の傾向 | 属性との関係 |
# MAGIC | 質問 | 「何が多いか？」 | 「誰がどう感じているか？」 |
# MAGIC | 例 | 「価格の不満が多い」 | 「20代が価格に不満」 |
# MAGIC | 施策 | 全体的な改善 | ターゲット別の施策 |

# COMMAND ----------


