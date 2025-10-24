# Databricks notebook source
# MAGIC %md
# MAGIC # 中級③：AIを活用した改善施策の自動生成
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC
# MAGIC これまでの分析で、データの傾向や問題点を把握しました。
# MAGIC 今回は、その分析結果を使って**AIに改善施策を提案させる**方法を学びます。
# MAGIC
# MAGIC ### なぜAIに施策を提案させるのか？
# MAGIC
# MAGIC #### 従来の方法の課題
# MAGIC - 人間が一つ一つ考えるのは時間がかかる
# MAGIC - 専門知識がないと良い案が出せない
# MAGIC - 複数のカテゴリを同時に検討するのは大変
# MAGIC
# MAGIC #### AIを使うメリット
# MAGIC - **スピード**：数秒で複数の施策案を生成
# MAGIC - **網羅性**：様々な角度からの提案が得られる
# MAGIC - **スケール**：多数のカテゴリに対して一括処理
# MAGIC - **叩き台**：人間が考える際の出発点になる
# MAGIC
# MAGIC ### このノートブックの流れ
# MAGIC 1. **問題の特定**：どのカテゴリの不満が多いか確認
# MAGIC 2. **プロンプト作成**：AIへの質問文を作成
# MAGIC 3. **AI実行**：AIに施策を提案させる
# MAGIC 4. **結果の整理**：提案を見やすく表示
# MAGIC
# MAGIC ### 注意：AIの役割
# MAGIC AIの提案は「叩き台」です。
# MAGIC 最終的な判断は、人間が業務知識や実現可能性を考慮して行います。

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ1：必要なライブラリのインストール
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC AIの出力（Markdown形式）を見やすく表示するためのライブラリをインストールします。
# MAGIC
# MAGIC ### markdownライブラリとは？
# MAGIC Markdown形式のテキストをHTML（Webページ形式）に変換するツールです。
# MAGIC これにより、AIの出力を整形して表示できます。

# COMMAND ----------

# markdownライブラリをインストール
%pip install markdown

# ライブラリを有効化するためにPythonを再起動
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ2：分析環境の準備
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

print("分析環境の準備完了")

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
# MAGIC ## ステップ4：改善対象カテゴリの特定
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC ネガティブカテゴリ（不満の種類）ごとの出現件数を集計し、
# MAGIC **どの不満が多いか**を確認します。
# MAGIC
# MAGIC ### なぜこれが重要？
# MAGIC 不満が多いカテゴリから優先的に改善することで、
# MAGIC 効率的に顧客満足度を向上できます。
# MAGIC
# MAGIC ### 分析の視点
# MAGIC - 出現件数が多い = 多くの人が感じている問題
# MAGIC - 優先的に改善すべき課題

# COMMAND ----------

from pyspark.sql import functions as F

# ネガティブカテゴリごとの出現件数を集計
negative_distribution = (
    gold_df
    .groupBy("negative_feedback_category")
    .agg(F.count("*").alias("出現件数"))
    .orderBy(F.desc("出現件数"))  # 件数が多い順に並べ替え
)

print("=" * 60)
print("ネガティブカテゴリ別の出現件数（上位5件）")
print("=" * 60)
display(negative_distribution.limit(5))

print("\nこの上位カテゴリに対して、AIに改善施策を提案させます")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ5：AIへの質問文（プロンプト）の作成
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 上位3つのネガティブカテゴリに対して、AIへの質問文を作成します。
# MAGIC
# MAGIC ### プロンプトとは？
# MAGIC AIへの「指示文」のことです。
# MAGIC 良いプロンプトを書くことで、AIから良い回答を引き出せます。
# MAGIC
# MAGIC ### プロンプト設計のポイント
# MAGIC 1. **背景情報**：何についての質問か明確にする
# MAGIC 2. **具体的な依頼**：何を出力してほしいか明示する
# MAGIC 3. **形式指定**：出力形式（Markdown、箇条書きなど）を指定
# MAGIC 4. **制約条件**：施策の数、文字数などを指定
# MAGIC
# MAGIC ### このノートブックのプロンプト構成
# MAGIC - 背景：「このカテゴリの不満が多い」
# MAGIC - 依頼：「改善施策を3つ提案して」
# MAGIC - 形式：「Markdown形式で出力」
# MAGIC - 制約：「1〜2年で実現可能なもの」

# COMMAND ----------

from pyspark.sql.functions import format_string, col

# 上位3つのカテゴリを取得
top_3_categories = negative_distribution.limit(3).select("negative_feedback_category")

# AIへの質問文のテンプレート
# %s の部分にカテゴリ名が入ります
prompt_template = """
過去のアンケート調査から、「%s」に関する不満・改善要望が多く見られました。

この傾向をもとに、今後1〜2年で最も有効と思われる**改善施策を3つ**提案してください。

各施策について、以下を含めてください：
1. 施策の具体的な内容
2. 期待される効果
3. 実施する理由・根拠

出力形式：Markdown形式で見やすく整理してください。

対象カテゴリ: "%s"
"""

# カテゴリごとにプロンプトを作成
prompts_df = top_3_categories.withColumn(
    "プロンプト",
    format_string(
        prompt_template,
        col("negative_feedback_category"),
        col("negative_feedback_category")
    )
).withColumnRenamed("negative_feedback_category", "カテゴリ")

print("=" * 60)
print("作成されたプロンプト")
print("=" * 60)
display(prompts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ6：AIによる施策提案の生成
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 作成したプロンプトをAIに送信し、改善施策の提案を受け取ります。
# MAGIC
# MAGIC ### ai_query関数とは？
# MAGIC Databricksが提供する、AIモデルを簡単に呼び出せる関数です。
# MAGIC
# MAGIC #### 使い方
# MAGIC ```sql
# MAGIC ai_query(
# MAGIC   'モデル名',
# MAGIC   プロンプト,
# MAGIC   オプション
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC ### このセルで使用するモデル
# MAGIC `databricks-gpt-oss-120b`：Databricksが提供する大規模言語モデル
# MAGIC
# MAGIC ### 実行時間について
# MAGIC AIの処理には時間がかかります（数秒〜数十秒）。
# MAGIC カテゴリ数が多いほど時間がかかります。
# MAGIC
# MAGIC ### 注意点
# MAGIC - AIの回答は毎回少し異なる場合があります
# MAGIC - 回答の品質はプロンプトの質に依存します
# MAGIC - 生成された内容は必ず人間が確認・検証してください

# COMMAND ----------

print("AIによる施策提案を生成中...")
print("（数十秒かかる場合があります）")

# AIに施策提案を生成させる
ai_proposals_df = prompts_df.selectExpr(
    "`カテゴリ`",
    """
    ai_query(
        'databricks-gpt-oss-120b',
        `プロンプト`,
        responseFormat => '{"type":"text"}'
    ) as `施策提案`
    """
)

print("✓ 施策提案の生成完了")
print("\n生成された施策提案:")
display(ai_proposals_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ7：施策提案の見やすい表示
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC AIが生成した施策提案を、Markdown形式で見やすく表示します。
# MAGIC
# MAGIC ### 表示の工夫
# MAGIC - カテゴリごとに区切って表示
# MAGIC - Markdownを整形してHTML表示
# MAGIC - 見出しや箇条書きを活用
# MAGIC
# MAGIC ### なぜ見やすさが重要？
# MAGIC - 複数のカテゴリの提案を比較しやすい
# MAGIC - 会議資料としてそのまま使える
# MAGIC - 関係者への共有がスムーズ

# COMMAND ----------

from markdown import markdown
from IPython.display import display as ipython_display, HTML

def display_markdown(markdown_text):
    """Markdown形式のテキストをHTMLに変換して表示"""
    html = markdown(markdown_text)
    ipython_display(HTML(html))

print("=" * 80)
print("カテゴリ別 改善施策提案")
print("=" * 80)

# 各カテゴリの提案を順番に表示
for row in ai_proposals_df.collect():
    category = row["カテゴリ"]
    proposal = row["施策提案"]
    
    # カテゴリ名を見出しとして追加
    markdown_content = f"# 📊 カテゴリ：{category}\n\n{proposal}"
    
    # Markdown形式で表示
    display_markdown(markdown_content)
    
    # 区切り線
    display_markdown("---")

print("\n" + "=" * 80)
print("すべての施策提案の表示が完了しました")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ8：施策提案の保存（オプション）
# MAGIC
# MAGIC ### このセルで行うこと
# MAGIC 生成された施策提案をテーブルとして保存します。
# MAGIC
# MAGIC ### なぜ保存するのか？
# MAGIC - 後から参照できる
# MAGIC - 他のメンバーと共有できる
# MAGIC - 時系列での変化を追跡できる
# MAGIC - BIツールで可視化できる
# MAGIC
# MAGIC ### 保存する情報
# MAGIC - カテゴリ名
# MAGIC - 施策提案の内容
# MAGIC - 生成日時
# MAGIC - 生成に使用したプロンプト

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# 生成日時を追加
proposals_with_timestamp = (
    ai_proposals_df
    .join(prompts_df, on="カテゴリ")
    .withColumn("生成日時", current_timestamp())
    .select(
        "`カテゴリ`",
        "`施策提案`",
        "`プロンプト`",
        "`生成日時`"
    )
)

# テーブル名
PROPOSALS_TABLE = "ai_generated_proposals"

# テーブルとして保存
proposals_with_timestamp.write.mode("overwrite").saveAsTable(PROPOSALS_TABLE)

print(f"✓ 施策提案を '{PROPOSALS_TABLE}' テーブルに保存しました")
print(f"保存件数: {proposals_with_timestamp.count()}件")

# 保存内容の確認
print("\n保存されたデータ:")
display(spark.table(PROPOSALS_TABLE))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ まとめ：この章で学んだこと
# MAGIC
# MAGIC ### 実施した内容
# MAGIC 1. **問題の特定**：ネガティブカテゴリの出現頻度を分析
# MAGIC 2. **プロンプト設計**：AIへの効果的な質問文を作成
# MAGIC 3. **AI実行**：ai_query関数で施策提案を自動生成
# MAGIC 4. **結果の整理**：Markdown形式で見やすく表示
# MAGIC 5. **データ保存**：後から参照できるようテーブル化
# MAGIC
# MAGIC ### AIを活用した分析の価値
# MAGIC
# MAGIC #### 従来の分析フロー
# MAGIC ```
# MAGIC データ分析 → 人間が考える → 施策立案
# MAGIC （数日〜数週間）
# MAGIC ```
# MAGIC
# MAGIC #### AI活用後のフロー
# MAGIC ```
# MAGIC データ分析 → AIが提案 → 人間が検証・改善 → 施策立案
# MAGIC （数時間〜1日）
# MAGIC ```
# MAGIC
# MAGIC ### 実務での活用例
# MAGIC
# MAGIC #### マーケティング部門
# MAGIC - 顧客の不満に対する改善施策を迅速に立案
# MAGIC - 複数のセグメントに対して同時に施策検討
# MAGIC - A/Bテストの仮説を素早く生成
# MAGIC
# MAGIC #### 商品開発部門
# MAGIC - 顧客フィードバックから改善アイデアを抽出
# MAGIC - 競合分析と組み合わせた差別化戦略の立案
# MAGIC - 新機能の優先順位付けの参考情報
# MAGIC
# MAGIC #### カスタマーサポート部門
# MAGIC - よくある問題に対する解決策の自動生成
# MAGIC - FAQ作成の効率化
# MAGIC - サポート品質向上のための施策立案
# MAGIC
# MAGIC ### AIを使う際の注意点
# MAGIC
# MAGIC #### 1. AIの限界を理解する
# MAGIC - AIは過去のデータから学習している
# MAGIC - 最新の情報や特殊な状況は反映されていない
# MAGIC - 業界特有の知識は不足している可能性がある
# MAGIC
# MAGIC #### 2. 必ず人間が検証する
# MAGIC - 実現可能性の確認
# MAGIC - コストの妥当性
# MAGIC - 法律・規制への適合性
# MAGIC - 企業の方針との整合性
# MAGIC
# MAGIC #### 3. プロンプトの品質が重要
# MAGIC - 具体的な指示を与える
# MAGIC - 背景情報を十分に提供する
# MAGIC - 出力形式を明確に指定する

# COMMAND ----------

# MAGIC %md
# MAGIC ---
