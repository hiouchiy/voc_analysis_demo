# Databricks notebook source
# MAGIC %md
# MAGIC # アンケートデータ分析パイプライン構築ハンズオン
# MAGIC
# MAGIC ## このノートブックで学ぶこと
# MAGIC
# MAGIC このハンズオンでは、Webから取得したアンケートデータを、
# MAGIC Databricks上で段階的に整備・分析できる形に変換する方法を学びます。
# MAGIC
# MAGIC ### データパイプラインとは
# MAGIC
# MAGIC データパイプラインは、生データを分析可能な形に変換する一連の処理です。
# MAGIC 一般的に以下の3つの層（メダリオンアーキテクチャ）で構成されます：
# MAGIC
# MAGIC ```
# MAGIC 生データ → Bronze → Silver → Gold → 分析・AI活用
# MAGIC ```
# MAGIC
# MAGIC ### 各層の役割
# MAGIC
# MAGIC #### Bronze層（青銅層）
# MAGIC - **目的**: 生データをそのまま保存
# MAGIC - **特徴**: 最小限の変換のみ
# MAGIC - **メリット**: いつでも元データに戻れる
# MAGIC
# MAGIC #### Silver層（銀層）
# MAGIC - **目的**: データのクリーニングと正規化
# MAGIC - **特徴**: 型変換、重複除去、値の統一
# MAGIC - **メリット**: 分析しやすい形式に整備
# MAGIC
# MAGIC #### Gold層（金層）
# MAGIC - **目的**: ビジネス用途に特化したデータ
# MAGIC - **特徴**: 集計、AI分析、レポート用
# MAGIC - **メリット**: すぐに使える形式
# MAGIC
# MAGIC ### 今回のパイプラインの流れ
# MAGIC
# MAGIC 1. **データ取得**: WebからアンケートJSONをダウンロード
# MAGIC 2. **Bronze層作成**: 生データをDeltaテーブルに保存
# MAGIC 3. **Silver層作成**: データの型変換と正規化
# MAGIC 4. **Gold層作成**: AIを使った高度な分析
# MAGIC    - 自由記述からの情報抽出
# MAGIC    - コメントの自動分類
# MAGIC
# MAGIC ### 使用する技術
# MAGIC
# MAGIC - **Unity Catalog**: Databricksのデータ管理システム
# MAGIC - **Delta Lake**: 高性能なデータストレージ形式
# MAGIC - **AI Functions**: DatabricksのAI機能（ai_query）

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ1: 環境設定
# MAGIC
# MAGIC ### パラメータの設定
# MAGIC
# MAGIC データを保存する場所や、使用するAIモデルなどを設定します。
# MAGIC
# MAGIC #### Unity Catalogの構造
# MAGIC
# MAGIC ```
# MAGIC Catalog（カタログ）
# MAGIC └── Schema（スキーマ）
# MAGIC     ├── Table（テーブル）
# MAGIC     └── Volume（ボリューム：ファイル保存用）
# MAGIC ```
# MAGIC
# MAGIC #### 各パラメータの説明
# MAGIC
# MAGIC - **CATALOG**: データベースの最上位グループ（例：部門名、プロジェクト名）
# MAGIC - **SCHEMA**: カタログ内の論理的なグループ（例：用途別）
# MAGIC - **VOLUME**: ファイルを保存する場所
# MAGIC - **SOURCE_URL**: アンケートデータのダウンロード元
# MAGIC - **LLM_ENDPOINT**: 使用するAIモデルの名前

# COMMAND ----------

# ===== 環境設定パラメータ =====
# ※ご自身の環境に合わせて変更してください

# Unity Catalog の設定
CATALOG = "handson"                      # カタログ名
SCHEMA  = "survey_analysis"              # スキーマ名
VOLUME  = "raw_data"                     # ボリューム名（ファイル保存用）

# データソース
SOURCE_URL = "https://raw.githubusercontent.com/hiouchiy/voc_analysis_demo/refs/heads/main/data/survey_responses.jsonl"

# ファイル保存先
DST_DIR  = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/landing"
DST_FILE = f"{DST_DIR}/survey_responses.jsonl"

# AIモデルの設定
LLM_ENDPOINT = "databricks-gpt-oss-120b"  # 使用するLLMエンドポイント名

# テーブル名
BRONZE_TABLE = "bronze_survey_responses"           # Bronze層テーブル
SILVER_TABLE = "silver_survey_responses_base"      # Silver層テーブル
GOLD_TABLE   = "gold_survey_responses_final"       # Gold層テーブル

print("✓ パラメータ設定完了")
print(f"  カタログ: {CATALOG}")
print(f"  スキーマ: {SCHEMA}")
print(f"  ボリューム: {VOLUME}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ2: Unity Catalogオブジェクトの作成
# MAGIC
# MAGIC ### Unity Catalogとは
# MAGIC
# MAGIC Databricksのデータ管理システムで、以下の機能を提供します：
# MAGIC
# MAGIC - **データガバナンス**: 誰がどのデータにアクセスできるか管理
# MAGIC - **データ系譜**: データがどこから来てどう変換されたか追跡
# MAGIC - **メタデータ管理**: データの説明や構造を一元管理
# MAGIC
# MAGIC ### 作成するオブジェクト
# MAGIC
# MAGIC 1. **Catalog**: データベースの最上位コンテナ
# MAGIC 2. **Schema**: テーブルをグループ化する論理的な単位
# MAGIC 3. **Volume**: ファイルを保存するための領域
# MAGIC
# MAGIC ### IF NOT EXISTS の意味
# MAGIC
# MAGIC 「もし存在しなければ作成する」という意味です。
# MAGIC 既に存在する場合はエラーにならず、そのまま使用します。

# COMMAND ----------

print("Unity Catalogオブジェクトを作成しています...")

# Catalog（カタログ）の作成
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
print(f"  ✓ カタログ '{CATALOG}' を確認/作成しました")

# Catalogを使用対象として選択
spark.sql(f"USE CATALOG {CATALOG}")
print(f"  ✓ カタログ '{CATALOG}' を選択しました")

# Schema（スキーマ）の作成
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")
print(f"  ✓ スキーマ '{SCHEMA}' を確認/作成しました")

# Schemaを使用対象として選択
spark.sql(f"USE {SCHEMA}")
print(f"  ✓ スキーマ '{SCHEMA}' を選択しました")

# Volume（ボリューム）の作成
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {VOLUME} 
    COMMENT 'アンケート生データの保存場所'
""")
print(f"  ✓ ボリューム '{VOLUME}' を確認/作成しました")

print("\n✓ Unity Catalogの準備が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ3: データのダウンロード
# MAGIC
# MAGIC ### JSON Linesとは
# MAGIC
# MAGIC JSON Lines（.jsonl）は、1行に1つのJSONオブジェクトを記述する形式です。
# MAGIC
# MAGIC **通常のJSON（配列形式）**:
# MAGIC ```json
# MAGIC [
# MAGIC   {"id": 1, "name": "太郎"},
# MAGIC   {"id": 2, "name": "花子"}
# MAGIC ]
# MAGIC ```
# MAGIC
# MAGIC **JSON Lines形式**:
# MAGIC ```json
# MAGIC {"id": 1, "name": "太郎"}
# MAGIC {"id": 2, "name": "花子"}
# MAGIC ```
# MAGIC
# MAGIC ### なぜJSON Linesを使うのか
# MAGIC
# MAGIC - 大きなファイルでも1行ずつ処理できる
# MAGIC - ストリーミング処理に適している
# MAGIC - ファイルの途中から読み込める
# MAGIC
# MAGIC ### ダウンロードの流れ
# MAGIC
# MAGIC 1. WebからデータをダウンロードHTTP経由でダウンロード）
# MAGIC 2. 一時ファイルに保存
# MAGIC 3. Unity Catalog Volumeに転送
# MAGIC 4. 一時ファイルを削除
# MAGIC
# MAGIC ### 注意事項
# MAGIC
# MAGIC ネットワーク制限がある環境では、ダウンロードが失敗する場合があります。
# MAGIC その場合は、手動でファイルをアップロードしてください。

# COMMAND ----------

import requests
import tempfile
import os

print("データをダウンロードしています...")

# 保存先ディレクトリの作成
try:
    dbutils.fs.mkdirs(DST_DIR)
    print(f"  ✓ 保存先ディレクトリを作成しました: {DST_DIR}")
except Exception as e:
    print(f"  ℹ ディレクトリは既に存在します: {DST_DIR}")

# Webからデータをダウンロード
try:
    print(f"  ダウンロード中: {SOURCE_URL}")
    response = requests.get(SOURCE_URL, timeout=60)
    response.raise_for_status()  # エラーがあれば例外を発生
    content = response.content
    print(f"  ✓ ダウンロード完了: {len(content)} バイト")
    
except requests.exceptions.RequestException as e:
    print(f"  ✗ ダウンロードエラー: {e}")
    print("\n【対処方法】")
    print("1. ネットワーク接続を確認してください")
    print("2. または、以下の手順で手動アップロードしてください：")
    print(f"   a. 左サイドバーの 'Catalog' をクリック")
    print(f"   b. {CATALOG} > {SCHEMA} > {VOLUME} に移動")
    print(f"   c. 'Upload' ボタンからファイルをアップロード")
    raise

# 一時ファイルに保存
temp_file = tempfile.mktemp(suffix=".jsonl")
print(f"  一時ファイルに保存中: {temp_file}")

try:
    with open(temp_file, "wb") as f:
        f.write(content)
    print(f"  ✓ 一時ファイルに保存しました")
    
    # Unity Catalog Volumeに転送
    with open(temp_file, "r", encoding="utf-8") as f:
        dbutils.fs.put(DST_FILE, f.read(), overwrite=True)
    print(f"  ✓ Volumeに保存しました: {DST_FILE}")
    
finally:
    # 一時ファイルを削除
    if os.path.exists(temp_file):
        os.remove(temp_file)
        print(f"  ✓ 一時ファイルを削除しました")

print("\n✓ データのダウンロードが完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ4: Bronze層の作成
# MAGIC
# MAGIC ### Bronze層とは
# MAGIC
# MAGIC Bronze層は、生データをほぼそのままの形で保存する層です。
# MAGIC
# MAGIC **Bronze層の原則**:
# MAGIC - 元データの形式を可能な限り保持
# MAGIC - 最小限の型変換のみ実施
# MAGIC - データの欠損や不正値もそのまま保存
# MAGIC - 「いつでも元に戻れる」状態を維持
# MAGIC
# MAGIC ### 処理の流れ
# MAGIC
# MAGIC 1. **JSON Linesファイルの読み込み**
# MAGIC    - Sparkで1行ずつJSONとして解析
# MAGIC    - 自動的にスキーマを推論
# MAGIC
# MAGIC 2. **基本的な型変換**
# MAGIC    - 日時文字列 → タイムスタンプ型
# MAGIC    - 真偽値文字列 → Boolean型
# MAGIC
# MAGIC 3. **Deltaテーブルとして保存**
# MAGIC    - トランザクション対応
# MAGIC    - タイムトラベル機能（過去のバージョンに戻せる）
# MAGIC    - 高速なクエリ性能

# COMMAND ----------

from pyspark.sql import functions as F

print("Bronze層を作成しています...")

# ステップ1: JSON Linesファイルの読み込み
print(f"  データを読み込んでいます: {DST_FILE}")

df_bronze = (
    spark.read
    .option("multiLine", "false")  # JSON Lines形式（1行=1JSON）
    .json(DST_FILE)
)

# 全レコードの10%をサンプリング
df_bronze = df_bronze.sample(fraction=0.1, seed=42)

print(f"  ✓ {df_bronze.count()} 件のレコードを読み込みました（10%サンプル）")

# データ構造を確認
print("\n  データ構造:")
df_bronze.printSchema()

# 最初の数件を表示
print("\n  データのサンプル:")
display(df_bronze.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 【課題①】型変換とテーブル作成
# MAGIC
# MAGIC ここで、実際に手を動かして学びましょう！
# MAGIC
# MAGIC #### やること
# MAGIC
# MAGIC 以下の2つの列の型を変換して、Deltaテーブルを作成してください：
# MAGIC
# MAGIC 1. **submitted_at列**: 文字列 → タイムスタンプ型
# MAGIC    - 形式: "yyyy-MM-dd HH:mm:ss"
# MAGIC    - 使用関数: `F.to_timestamp()`
# MAGIC
# MAGIC 2. **drives_weekly列**: 文字列 → Boolean型
# MAGIC    - 使用関数: `.cast("boolean")`
# MAGIC
# MAGIC 3. **Deltaテーブルとして保存**
# MAGIC    - テーブル名: `BRONZE_TABLE`
# MAGIC    - モード: overwrite（上書き）
# MAGIC
# MAGIC #### ヒント
# MAGIC
# MAGIC ```python
# MAGIC # 列の型変換の例
# MAGIC df = df.withColumn("列名", F.to_timestamp("列名", "日付形式"))
# MAGIC df = df.withColumn("列名", F.col("列名").cast("型"))
# MAGIC
# MAGIC # Deltaテーブルへの保存の例
# MAGIC df.write.format("delta").mode("モード").saveAsTable("テーブル名")
# MAGIC ```
# MAGIC
# MAGIC #### 制限時間
# MAGIC
# MAGIC 5分程度で挑戦してみてください。
# MAGIC わからない場合は、次のセルの正解例を確認してください。

# COMMAND ----------

# DBTITLE 1,【課題①】ここにコードを書いてください
# ===== ここにコードを書いてください =====

# ステップ1: submitted_at列をタイムスタンプ型に変換


# ステップ2: drives_weekly列をBoolean型に変換


# ステップ3: Deltaテーブルとして保存


# ===== ここまで =====

# COMMAND ----------

# MAGIC %md
# MAGIC ### 【課題①の正解例】
# MAGIC
# MAGIC まずは自分で考えてから見てください！
# MAGIC
# MAGIC #### 解説
# MAGIC
# MAGIC 1. **withColumn()**: 既存の列を変換または新しい列を追加
# MAGIC 2. **F.to_timestamp()**: 文字列を日時型に変換
# MAGIC 3. **cast()**: データ型を変換
# MAGIC 4. **write.format("delta")**: Delta形式で保存
# MAGIC 5. **mode("overwrite")**: 既存データを上書き
# MAGIC 6. **option("overwriteSchema", "true")**: スキーマの変更も許可

# COMMAND ----------

# DBTITLE 1,【課題①の正解例】
from pyspark.sql import functions as F

print("Bronze層のデータを変換しています...")

# ステップ1: 型変換
df_bronze_transformed = (
    df_bronze
    # submitted_at列を文字列からタイムスタンプ型に変換
    .withColumn(
        "submitted_at", 
        F.to_timestamp("submitted_at", "yyyy-MM-dd HH:mm:ss")
    )
    # drives_weekly列を文字列からBoolean型に変換
    .withColumn(
        "drives_weekly", 
        F.col("drives_weekly").cast("boolean")
    )
)

print("  ✓ 型変換が完了しました")

# 変換後のスキーマを確認
print("\n  変換後のデータ構造:")
df_bronze_transformed.printSchema()

# ステップ2: Deltaテーブルとして保存
print(f"\n  Deltaテーブルに保存しています: {BRONZE_TABLE}")

(
    df_bronze_transformed
    .write
    .format("delta")                      # Delta形式で保存
    .mode("overwrite")                    # 既存データを上書き
    .option("overwriteSchema", "true")    # スキーマの変更を許可
    .saveAsTable(BRONZE_TABLE)
)

print(f"  ✓ Bronzeテーブルを作成しました: {CATALOG}.{SCHEMA}.{BRONZE_TABLE}")

# テーブルの件数を確認
record_count = spark.table(BRONZE_TABLE).count()
print(f"  ✓ 保存されたレコード数: {record_count} 件")

print("\n✓ Bronze層の作成が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Bronzeテーブルの確認
# MAGIC
# MAGIC 作成したテーブルの内容を確認しましょう。

# COMMAND ----------

# -- Bronzeテーブルの最初の10件を表示
spark.sql(f"SELECT * FROM {BRONZE_TABLE} LIMIT 10;").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ5: Silver層の作成
# MAGIC
# MAGIC ### Silver層とは
# MAGIC
# MAGIC Silver層は、データをクリーニングして分析可能な形に整備する層です。
# MAGIC
# MAGIC **Silver層で行うこと**:
# MAGIC - ✓ データ型の正規化
# MAGIC - ✓ 不正な値の修正またはデフォルト値への置換
# MAGIC - ✓ 重複データの除去
# MAGIC - ✓ カテゴリ値の統一
# MAGIC - ✓ 配列やJSON構造の整形
# MAGIC
# MAGIC ### 今回の正規化内容
# MAGIC
# MAGIC #### 1. カテゴリ値の正規化
# MAGIC
# MAGIC 各カテゴリ列に対して、許可された値のリストを定義し、
# MAGIC それ以外の値は"unknown"や"no_answer"などのデフォルト値に置換します。
# MAGIC
# MAGIC **例**: age_group列
# MAGIC - 許可値: u19, 20s, 30s, 40s, 50s, 60plus
# MAGIC - "30代" → "30s"に正規化
# MAGIC - "不明" → "unknown"に置換
# MAGIC
# MAGIC #### 2. 配列型への変換
# MAGIC
# MAGIC カンマ区切りの文字列を配列型に変換します。
# MAGIC
# MAGIC **例**: fav_alcohols列
# MAGIC - 元データ: "beer, wine, sake"
# MAGIC - 変換後: ["beer", "wine", "sake"]
# MAGIC
# MAGIC #### 3. 数値型への変換
# MAGIC
# MAGIC 満足度などの文字列を整数型に変換します。
# MAGIC
# MAGIC **例**: satisfaction列
# MAGIC - "5" → 5（整数）
# MAGIC - "とても満足" → NULL（数値でない場合）
# MAGIC
# MAGIC #### 4. 重複の除去
# MAGIC
# MAGIC 同じIDのレコードが複数ある場合、最新のsubmitted_atのみを残します。

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

print("Silver層を作成しています...")

# Bronzeテーブルを読み込み
df_silver = spark.table(BRONZE_TABLE)
print(f"  ✓ Bronzeテーブルを読み込みました: {df_silver.count()} 件")

# ===== ユーティリティ関数の定義 =====

def to_clean_array(column_name):
    """
    カンマ区切りの文字列を配列に変換し、クリーニングする関数
    
    処理内容:
    1. カンマで分割
    2. 各要素の前後の空白を削除
    3. 小文字に統一
    4. 重複を除去
    5. 空文字列を除外
    
    引数:
        column_name: 変換対象の列名
    
    戻り値:
        クリーニングされた配列
    """
    return F.expr(f"""
        filter(
            array_distinct(
                transform(
                    split(coalesce({column_name}, ''), ','), 
                    x -> trim(lower(x))
                )
            ), 
            x -> x <> ''
        )
    """)


def normalize_enum(column_name, allowed_values, default_value="unknown"):
    """
    カテゴリ値を正規化する関数
    
    処理内容:
    1. NULL値や空文字をデフォルト値に置換
    2. 許可された値のリストと照合
    3. 一致しない場合はデフォルト値に置換
    4. 小文字に統一
    
    引数:
        column_name: 対象列名
        allowed_values: 許可される値のリスト
        default_value: デフォルト値
    
    戻り値:
        正規化された値
    """
    # 小文字に変換した許可値リスト
    allowed_lower = [v.lower() for v in allowed_values]
    
    return (
        F.when(
            F.col(column_name).isNull() | (F.trim(F.col(column_name)) == ""),
            F.lit(default_value)
        )
        .otherwise(
            F.when(
                F.lower(F.col(column_name)).isin(allowed_lower),
                F.lower(F.col(column_name))
            )
            .otherwise(F.lit(default_value))
        )
    )

print("  ✓ ユーティリティ関数を定義しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ### カテゴリ値の定義
# MAGIC
# MAGIC 各カテゴリ列に対して、許可される値のリストを定義します。

# COMMAND ----------

# ===== 許可されるカテゴリ値の定義 =====

# 年代グループ
allowed_age_groups = ["u19", "20s", "30s", "40s", "50s", "60plus"]

# 性別
allowed_genders = ["male", "female", "other", "no_answer"]

# 地域
allowed_regions = [
    "hokkaido_to_hoku",    # 北海道・東北
    "kanto",                # 関東
    "chubu",                # 中部
    "kinki",                # 近畿
    "chugoku_shikoku",      # 中国・四国
    "kyushu_okinawa",       # 九州・沖縄
    "overseas",             # 海外
    "no_answer"             # 無回答
]

# 職業
allowed_occupations = [
    "company_employee",     # 会社員
    "self_employed",        # 自営業
    "student",              # 学生
    "homemaker",            # 主婦・主夫
    "other",                # その他
    "no_answer"             # 無回答
]

# 飲酒頻度
allowed_drinking_freq = ["daily", "weekly", "monthly", "never"]

# ノンアルコール飲用頻度
allowed_nonalc_freq = ["first_time", "sometimes", "weekly"]

# 価格帯
allowed_price_bands = [
    "under199",     # 199円以下
    "200_249",      # 200-249円
    "250_299",      # 250-299円
    "300_349",      # 300-349円
    "350plus"       # 350円以上
]

# 購入意向
allowed_purchase_intent = ["def_buy", "maybe", "unsure", "no"]

print("✓ カテゴリ値の定義が完了しました")

# COMMAND ----------

# MAGIC %md
# MAGIC ### データの正規化処理
# MAGIC
# MAGIC 定義したルールに基づいて、データを正規化します。

# COMMAND ----------

print("データを正規化しています...")

# ===== 型変換と正規化 =====

df_silver = (
    df_silver
    
    # 基本的な型変換
    .withColumn("id", F.col("id").cast("bigint"))
    .withColumn("submitted_at", F.col("submitted_at").cast("timestamp"))
    .withColumn("drives_weekly", F.col("drives_weekly").cast("boolean"))
    
    # カテゴリ値の正規化
    .withColumn("age_group", 
                normalize_enum("age_group", allowed_age_groups))
    .withColumn("gender", 
                normalize_enum("gender", allowed_genders, "no_answer"))
    .withColumn("region", 
                normalize_enum("region", allowed_regions, "no_answer"))
    .withColumn("occupation", 
                normalize_enum("occupation", allowed_occupations, "other"))
    .withColumn("drinking_freq", 
                normalize_enum("drinking_freq", allowed_drinking_freq))
    .withColumn("nonalc_freq", 
                normalize_enum("nonalc_freq", allowed_nonalc_freq))
    .withColumn("price_band", 
                normalize_enum("price_band", allowed_price_bands, "unknown"))
    .withColumn("purchase_intent", 
                normalize_enum("purchase_intent", allowed_purchase_intent))
    
    # 満足度を整数型に変換（1-5の範囲のみ許可）
    .withColumn("satisfaction_int",
        F.when(
            F.col("satisfaction").rlike("^[1-5]$"),  # 1-5の数字のみ
            F.col("satisfaction").cast("int")
        ).otherwise(F.lit(None).cast("int"))
    )
    
    # カンマ区切り文字列を配列に変換
    .withColumn("fav_alcohols_arr", to_clean_array("fav_alcohols"))
    .withColumn("health_reasons_arr", to_clean_array("health_reasons"))
    
    # 元の列を削除（配列版に置き換えたため）
    .drop("fav_alcohols", "health_reasons", "satisfaction")
)

print("  ✓ データの正規化が完了しました")

# 正規化後のスキーマを確認
print("\n  正規化後のデータ構造:")
df_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 重複データの除去
# MAGIC
# MAGIC #### なぜ重複が発生するのか
# MAGIC
# MAGIC - ユーザーが誤って複数回送信
# MAGIC - システムの不具合で二重登録
# MAGIC - データ統合時の問題
# MAGIC
# MAGIC #### 重複除去の方針
# MAGIC
# MAGIC 同じIDのレコードが複数ある場合：
# MAGIC 1. submitted_at（提出日時）で降順にソート
# MAGIC 2. 最新のレコードのみを残す
# MAGIC 3. 古いレコードは削除
# MAGIC
# MAGIC #### Window関数とは
# MAGIC
# MAGIC Window関数は、グループ内での順位付けや集計を行う機能です。
# MAGIC
# MAGIC **例**:
# MAGIC ```
# MAGIC ID | submitted_at        | row_number
# MAGIC ---|---------------------|------------
# MAGIC 1  | 2024-01-01 10:00:00 | 1  ← 最新（残す）
# MAGIC 1  | 2024-01-01 09:00:00 | 2  ← 古い（削除）
# MAGIC 2  | 2024-01-01 11:00:00 | 1  ← 最新（残す）
# MAGIC ```

# COMMAND ----------

print("重複データを除去しています...")

# Window関数の定義
# - partitionBy("id"): IDごとにグループ化
# - orderBy(...desc_nulls_last()): submitted_atの降順でソート（NULLは最後）
window_spec = Window.partitionBy("id").orderBy(
    F.col("submitted_at").desc_nulls_last()
)

# 重複除去の実行
df_silver = (
    df_silver
    # 各IDグループ内で行番号を付与
    .withColumn("_row_number", F.row_number().over(window_spec))
    # 行番号が1（最新）のみを残す
    .filter(F.col("_row_number") == 1)
    # 作業用の列を削除
    .drop("_row_number")
)

print(f"  ✓ 重複除去後のレコード数: {df_silver.count()} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silverテーブルとして保存
# MAGIC
# MAGIC 正規化したデータをDeltaテーブルとして保存します。

# COMMAND ----------

print(f"Silverテーブルに保存しています: {SILVER_TABLE}")

# Deltaテーブルとして保存
(
    df_silver
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"  ✓ Silverテーブルを作成しました: {CATALOG}.{SCHEMA}.{SILVER_TABLE}")

# テーブルプロパティの設定
spark.sql(f"""
    ALTER TABLE {SILVER_TABLE} 
    SET TBLPROPERTIES (
        'purpose' = 'silver_base',
        'description' = '型正規化済みアンケートデータ（AI抽出前）'
    )
""")

print("  ✓ テーブルプロパティを設定しました")

# テーブルの統計情報を表示
record_count = spark.table(SILVER_TABLE).count()
print(f"\n✓ Silver層の作成が完了しました")
print(f"  最終レコード数: {record_count} 件")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silverテーブルの確認

# COMMAND ----------

spark.sql(f"""
-- Silverテーブルの内容を確認
SELECT 
  id,
  submitted_at,
  age_group,
  gender,
  satisfaction_int,
  fav_alcohols_arr,
  free_comment
FROM {SILVER_TABLE}
LIMIT 10;""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ6: Gold層の作成（AI分析）
# MAGIC
# MAGIC ### Gold層とは
# MAGIC
# MAGIC Gold層は、ビジネス用途に特化した高度な分析データを提供する層です。
# MAGIC
# MAGIC **Gold層の特徴**:
# MAGIC - ビジネス指標の計算
# MAGIC - AI/MLによる高度な分析
# MAGIC - レポート用の集計データ
# MAGIC - すぐに使える形式
# MAGIC
# MAGIC ### 今回のGold層で行うこと
# MAGIC
# MAGIC #### 1. 自由記述からの情報抽出
# MAGIC
# MAGIC AIモデルを使って、自由記述コメント（free_comment）から以下を抽出：
# MAGIC - **positive_point**: ポジティブな意見
# MAGIC - **negative_feedback**: ネガティブな意見・要望
# MAGIC - **scene**: 飲用シーン
# MAGIC
# MAGIC #### 2. コメントの自動分類
# MAGIC
# MAGIC 抽出したコメントを18のカテゴリに自動分類：
# MAGIC
# MAGIC **領域1: Sensory Quality（製品の味わい）**
# MAGIC - Appearance（外観）
# MAGIC - Aroma（香り）
# MAGIC - Flavor（味）
# MAGIC - MouthfeelCarbonation（口当たり・炭酸）
# MAGIC - DrinkabilityAftertaste（飲みやすさ・後味）
# MAGIC
# MAGIC **領域2: Functional Health & Safety（健康・安全性）**
# MAGIC - ABV（アルコール度数）
# MAGIC - CaloriesCarbs（カロリー・糖質）
# MAGIC - AdditivesAllergen（添加物・アレルゲン）
# MAGIC - Ingredients（原材料）
# MAGIC
# MAGIC **領域3: Practical Usability（使い勝手）**
# MAGIC - PackagingDesign（パッケージデザイン）
# MAGIC - PackServingFormat（容量・形態）
# MAGIC - ShelfLifeStorage（保存性）
# MAGIC
# MAGIC **領域4: Economic & Availability（経済性・入手性）**
# MAGIC - PriceValue（価格・コスパ）
# MAGIC - Availability（入手しやすさ）
# MAGIC - PromotionVisibility（販促・広告）
# MAGIC
# MAGIC **領域5: Brand & Emotional Perception（ブランド・感情）**
# MAGIC - BrandImageStory（ブランドイメージ）
# MAGIC - SocialAcceptability（社会的受容性）
# MAGIC - SustainabilityEthics（持続可能性・倫理）
# MAGIC
# MAGIC ### Databricks AI Functionsとは
# MAGIC
# MAGIC DatabricksのAI Functionsは、SQLから直接AIモデルを呼び出せる機能です。
# MAGIC
# MAGIC **主な関数**:
# MAGIC - **ai_query()**: LLMに質問して回答を取得
# MAGIC - **ai_extract()**: テキストから構造化データを抽出
# MAGIC - **ai_classify()**: テキストを分類
# MAGIC
# MAGIC **メリット**:
# MAGIC - SQLだけでAI処理が可能
# MAGIC - 大量データの並列処理
# MAGIC - 結果がテーブルに直接保存される

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ6-1: 情報抽出用プロンプトの作成
# MAGIC
# MAGIC AIモデルに渡すプロンプト（指示文）を作成します。
# MAGIC
# MAGIC #### プロンプトエンジニアリングのポイント
# MAGIC
# MAGIC 1. **明確な指示**: 何を抽出するか具体的に記述
# MAGIC 2. **出力形式の指定**: 構造化データとして出力
# MAGIC 3. **制約条件の明示**: 簡潔に、重複なく、など
# MAGIC 4. **例の提示**: 期待する出力の例を示す

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== ステップ1: 情報抽出用プロンプトの作成 =====
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW _nlp_prompt AS
# MAGIC SELECT
# MAGIC   id,
# MAGIC   free_comment,
# MAGIC   CONCAT(
# MAGIC     '以下の日本語の感想文から情報抽出してください。',
# MAGIC     '\n\n【出力仕様】',
# MAGIC     '\n- positive_point: 感想の中のポジティブな意見のうち、最も強調されているものを短文で一つ抽出してください（例: 香りがよい、後味がすっきり）。',
# MAGIC     '  同様の内容は一つの文章にまとめ、サマライズして、簡潔にしてください。日本語で出力してください。句読点は不要です。',
# MAGIC     '  ただし、シーンに関するポジティブな言及はscenesに含め、positive_pointには含めないでください。',
# MAGIC     '\n- negative_feedback: ネガティブな意見、または、要望や提案のうち、最も強調されているものを短文で一つ抽出してください。',
# MAGIC     '  同様の内容は一つの文章にまとめ、サマライズして、簡潔にしてください。',
# MAGIC     '  原文の言葉表現がカジュアルな場合は、同じ意味のビジネスライクな表現に書き直してください。',
# MAGIC     '  日本語で出力してください。句読点は不要です。',
# MAGIC     '\n- scene: 飲用シーン（例: 夕食、風呂上がり、運転前、スポーツ後 など）のうち、最も強調されているものを単語で一つ出力してください。',
# MAGIC     '  同様の内容は一つのワードにまとめ、サマライズして、簡潔にしてください。日本語で出力してください。句読点は不要です。',
# MAGIC     '\n\n【出力形式】',
# MAGIC     '\n構造化: positive_point, negative_feedback, scene',
# MAGIC     '\n\n【対象の感想文】\n',
# MAGIC     COALESCE(free_comment, '')
# MAGIC   ) AS prompt
# MAGIC FROM silver_survey_responses_base
# MAGIC WHERE COALESCE(TRIM(free_comment), '') <> '';  -- 空のコメントは除外
# MAGIC
# MAGIC -- プロンプトの確認
# MAGIC SELECT id, LEFT(prompt, 200) AS prompt_preview
# MAGIC FROM _nlp_prompt
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ6-2: AIモデルによる情報抽出
# MAGIC
# MAGIC #### ai_query()関数の使い方
# MAGIC
# MAGIC ```sql
# MAGIC ai_query(
# MAGIC     'モデルエンドポイント名',
# MAGIC     プロンプト,
# MAGIC     responseFormat => '出力スキーマ'
# MAGIC )
# MAGIC ```
# MAGIC
# MAGIC #### responseFormatの指定
# MAGIC
# MAGIC STRUCTを使って、期待する出力の構造を定義します：
# MAGIC
# MAGIC ```sql
# MAGIC STRUCT<
# MAGIC   feedback_extraction:STRUCT<
# MAGIC     positive_point: STRING,
# MAGIC     negative_feedback: STRING,
# MAGIC     scene: STRING
# MAGIC   >
# MAGIC >
# MAGIC ```
# MAGIC
# MAGIC これにより、AIの出力が構造化され、列として簡単に扱えます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== ステップ2: AIモデルで情報抽出 =====
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW _nlp_ai_raw AS
# MAGIC SELECT
# MAGIC   p.id,
# MAGIC   p.free_comment,
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-120b',  -- ←ご自身のエンドポイント名に変更
# MAGIC     p.prompt,
# MAGIC     responseFormat => 'STRUCT<
# MAGIC       feedback_extraction:STRUCT<
# MAGIC         positive_point: STRING,
# MAGIC         negative_feedback: STRING,
# MAGIC         scene: STRING
# MAGIC       >
# MAGIC     >'
# MAGIC   ) AS ai_output
# MAGIC FROM _nlp_prompt p;
# MAGIC
# MAGIC -- 抽出結果の確認
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   LEFT(free_comment, 50) AS comment_preview,
# MAGIC   ai_output
# MAGIC FROM _nlp_ai_raw
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ6-3: 抽出結果の展開とビュー作成
# MAGIC
# MAGIC AIの出力（STRUCT型）を個別の列に展開します。
# MAGIC
# MAGIC #### STRUCT型のアクセス方法
# MAGIC
# MAGIC ```sql
# MAGIC -- ドット記法でネストした値にアクセス
# MAGIC ai_output.feedback_extraction.positive_point
# MAGIC
# MAGIC -- またはコロン記法
# MAGIC ai_output:feedback_extraction:positive_point
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== ステップ3: 抽出結果を列として展開 =====
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW gold_survey_responses_nlp AS
# MAGIC SELECT
# MAGIC   b.id,
# MAGIC   b.submitted_at,
# MAGIC   b.age_group,
# MAGIC   b.gender,
# MAGIC   b.occupation,
# MAGIC   b.region,
# MAGIC   b.drives_weekly,
# MAGIC   b.drinking_freq,
# MAGIC   b.fav_alcohols_arr,
# MAGIC   b.nonalc_freq,
# MAGIC   b.health_reasons_arr,
# MAGIC   b.purchase_intent,
# MAGIC   b.price_band,
# MAGIC   b.satisfaction_int,
# MAGIC   b.free_comment,
# MAGIC   -- AI抽出結果を個別の列として展開
# MAGIC   r.ai_output:feedback_extraction:positive_point     AS positive_point,
# MAGIC   r.ai_output:feedback_extraction:negative_feedback  AS negative_feedback,
# MAGIC   r.ai_output:feedback_extraction:scene              AS scene
# MAGIC FROM silver_survey_responses_base b
# MAGIC LEFT JOIN _nlp_ai_raw r ON b.id = r.id;
# MAGIC
# MAGIC -- 結果の確認
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   LEFT(free_comment, 50) AS comment_preview,
# MAGIC   positive_point,
# MAGIC   negative_feedback,
# MAGIC   scene
# MAGIC FROM gold_survey_responses_nlp
# MAGIC WHERE positive_point IS NOT NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ6-4: コメントの自動分類
# MAGIC
# MAGIC 抽出したpositive_pointとnegative_feedbackを、
# MAGIC 18のカテゴリに自動分類します。
# MAGIC
# MAGIC #### 分類ガイドラインの設計
# MAGIC
# MAGIC 各カテゴリに対して：
# MAGIC - 明確な定義
# MAGIC - キーワード例
# MAGIC - 除外例（他のカテゴリとの区別）
# MAGIC
# MAGIC これにより、AIが一貫性のある分類を行えるようになります。
# MAGIC
# MAGIC #### 処理の流れ
# MAGIC
# MAGIC 1. **ガイドラインテキストの準備**: 18カテゴリの詳細な定義
# MAGIC 2. **分類プロンプトの作成**: ガイドライン + コメント
# MAGIC 3. **AIモデルで分類実行**: カテゴリと信頼度を取得
# MAGIC 4. **結果の展開**: 分類結果を列として追加

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== ステップ4-1: 分類ガイドラインの定義 =====
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW guideline_text_view AS
# MAGIC SELECT '''
# MAGIC 以下のガイドに従って、コメントの内容を18個のクラスのいずれかに分類してください。
# MAGIC ただし、コメントが空白の場合はNoneを出力してください。
# MAGIC
# MAGIC # 指標分類ガイドライン
# MAGIC
# MAGIC ## 領域1: Sensory Quality ― 製品そのものの味わい
# MAGIC
# MAGIC ### Appearance（外観）
# MAGIC - 定義：色調、透明度、泡立ち、グラス映えなど外観に関する評価
# MAGIC - キーワード例：「黄金色」「濁りが少ない」「泡がすぐ消える」
# MAGIC - 除外例：缶の見た目やデザインは → PackagingDesign
# MAGIC
# MAGIC ### Aroma（香り）
# MAGIC - 定義：香り（ホップ、モルト、果実香、オフフレーバーなど）に関する記述
# MAGIC - キーワード例：「トロピカルな香り」「ホップが弱い」「香りが薬品っぽい」
# MAGIC - 除外例：味や後味の話 → Flavor / DrinkabilityAftertaste
# MAGIC
# MAGIC ### Flavor（味）
# MAGIC - 定義：甘味、苦味、酸味、コク、バランス、余韻など味覚の構成
# MAGIC - キーワード例：「麦の甘み」「苦みが強い」「人工甘味料っぽい味」
# MAGIC - 除外例：香りに関する話 → Aroma
# MAGIC
# MAGIC ### MouthfeelCarbonation（口当たり・炭酸）
# MAGIC - 定義：炭酸の強さ、粘度、ボディ感、舌触りなどの口当たり
# MAGIC - キーワード例：「クリーミーな泡」「ガス弱め」「軽い口当たり」
# MAGIC - 除外例：「飲みやすさ」は → DrinkabilityAftertaste
# MAGIC
# MAGIC ### DrinkabilityAftertaste（飲みやすさ・後味）
# MAGIC - 定義：飲みやすさ、ゴクゴク感、後味のキレや残り方
# MAGIC - キーワード例：「ゴクゴク飲める」「後味がべたつく」「キレがある」
# MAGIC - 除外例：味覚の詳細な記述 → Flavor
# MAGIC
# MAGIC ## 領域2: Functional Health & Safety ― "飲める理由" を決める要素
# MAGIC
# MAGIC ### ABV（アルコール度数）
# MAGIC - 定義：アルコール度数、0.00% 表記、法的基準
# MAGIC - キーワード例：「正真正銘0.00%」「0.3%なのが残念」
# MAGIC - 除外例：「酔わないから安心」→ SocialAcceptability 併用可
# MAGIC
# MAGIC ### CaloriesCarbs（カロリー・糖質）
# MAGIC - 定義：カロリー、糖質量、栄養成分表示の明確さ
# MAGIC - キーワード例：「100mlで8kcal」「糖質ゼロ」「栄養表示をもっと目立たせて」
# MAGIC - 除外例：甘さの味覚は → Flavor
# MAGIC
# MAGIC ### AdditivesAllergen（添加物・アレルゲン）
# MAGIC - 定義：保存料、香料、人工甘味料、アレルゲン（例：グルテン）
# MAGIC - キーワード例：「保存料不使用」「大麦アレルギー注意」
# MAGIC - 除外例：素材自体の良さや産地 → Ingredients
# MAGIC
# MAGIC ### Ingredients（原材料）
# MAGIC - 定義：使用されている素材そのものの質、産地、自然由来かどうか
# MAGIC - キーワード例：「国産モルト使用」「○○渓谷の水」
# MAGIC - 除外例：カロリーや添加物の話 → それぞれの該当項目へ
# MAGIC
# MAGIC ## 領域3: Practical Usability ― 使い勝手・シーン適合性
# MAGIC
# MAGIC ### PackagingDesign（パッケージデザイン）
# MAGIC - 定義：缶・瓶の色、手触り、ロゴ、開けやすさなど外装デザイン
# MAGIC - キーワード例：「マットな質感」「プルタブが硬い」「ラベルが可愛い」
# MAGIC - 除外例：内容量やパック数の話 → PackServingFormat
# MAGIC
# MAGIC ### PackServingFormat（容量・形態）
# MAGIC - 定義：容量バリエーション（250ml/350mlなど）やパッケージ形式（6本パックなど）
# MAGIC - キーワード例：「250mlスリム缶」「6本パックが欲しい」
# MAGIC - 除外例：価格そのものへの評価 → PriceValue
# MAGIC
# MAGIC ### ShelfLifeStorage（保存性）
# MAGIC - 定義：賞味期限、常温保存の可否、開栓後の劣化速度
# MAGIC - キーワード例：「常温保存OK」「開けたらすぐ劣化する」
# MAGIC - 除外例：外装の物理的な強度や見た目 → PackagingDesign
# MAGIC
# MAGIC ## 領域4: Economic & Availability ― 手に取りやすさ
# MAGIC
# MAGIC ### PriceValue（価格・コスパ）
# MAGIC - 定義：価格、コスパ、値ごろ感に対する評価
# MAGIC - キーワード例：「280円は高い」「この味でこの値段は安い」
# MAGIC - 除外例：パッケージサイズ・構成 → PackServingFormat
# MAGIC
# MAGIC ### Availability（入手しやすさ）
# MAGIC - 定義：購入のしやすさ、流通チャネル（コンビニ、ECなど）
# MAGIC - キーワード例：「どこにも売ってない」「オンライン限定」
# MAGIC - 除外例：値段が高い／安い → PriceValue
# MAGIC
# MAGIC ### PromotionVisibility（販促・広告）
# MAGIC - 定義：広告・店頭POP・販促イベントなどの目立ち方や訴求力
# MAGIC - キーワード例：「POPが目立つ」「試飲イベント良かった」
# MAGIC - 除外例：ブランドの歴史や理念 → BrandImageStory
# MAGIC
# MAGIC ## 領域5: Brand & Emotional Perception ― 心理的・社会的価値
# MAGIC
# MAGIC ### BrandImageStory（ブランドイメージ）
# MAGIC - 定義：ブランドの世界観、理念、クラフト感、物語性
# MAGIC - キーワード例：「クラフト感がある」「創業者の想いに共感」
# MAGIC - 除外例：具体的な環境配慮活動 → SustainabilityEthics
# MAGIC
# MAGIC ### SocialAcceptability（社会的受容性）
# MAGIC - 定義：運転中、妊娠中、仕事中などでも「飲んでOK」と思える感覚
# MAGIC - キーワード例：「運転前に飲めて安心」「会議中でも飲める」
# MAGIC - 除外例：ABVの数値のみの言及 → ABV（併用可）
# MAGIC
# MAGIC ### SustainabilityEthics（持続可能性・倫理）
# MAGIC - 定義：環境配慮、リサイクル素材、公正取引、企業倫理
# MAGIC - キーワード例：「再生アルミ缶」「CO₂削減」「ビーガン対応」
# MAGIC - 除外例：ブランドのイメージ全般 → BrandImageStory
# MAGIC ''' AS guideline_text;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ6-4-2: 分類プロンプトの作成

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== ステップ4-2: 分類プロンプトの作成 =====
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW _classification_prompt AS
# MAGIC SELECT
# MAGIC   r.id,
# MAGIC   r.positive_point,
# MAGIC   r.negative_feedback,
# MAGIC   -- ポジティブコメント用のプロンプト
# MAGIC   CONCAT(
# MAGIC     g.guideline_text,
# MAGIC     '\n\n【分類対象のポジティブなコメント】\n',
# MAGIC     COALESCE(r.positive_point, '')
# MAGIC   ) AS prompt_for_positive,
# MAGIC   -- ネガティブコメント用のプロンプト
# MAGIC   CONCAT(
# MAGIC     g.guideline_text,
# MAGIC     '\n\n【分類対象のネガティブなコメント】\n',
# MAGIC     COALESCE(r.negative_feedback, '')
# MAGIC   ) AS prompt_for_negative
# MAGIC FROM gold_survey_responses_nlp r
# MAGIC CROSS JOIN guideline_text_view g;
# MAGIC
# MAGIC -- プロンプトの確認
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   positive_point,
# MAGIC   LEFT(prompt_for_positive, 100) AS prompt_preview
# MAGIC FROM _classification_prompt
# MAGIC WHERE positive_point IS NOT NULL
# MAGIC LIMIT 3;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ6-4-3: AIモデルで分類実行
# MAGIC
# MAGIC #### 出力スキーマの設計
# MAGIC
# MAGIC ```sql
# MAGIC STRUCT<
# MAGIC   classification:STRUCT<
# MAGIC     category: STRING,      -- 分類カテゴリ名
# MAGIC     confidence: DOUBLE     -- 信頼度（0.0-1.0）
# MAGIC   >
# MAGIC >
# MAGIC ```
# MAGIC
# MAGIC 信頼度（confidence）を取得することで、
# MAGIC 分類結果の信頼性を評価できます。

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== ステップ4-3: AIモデルで分類実行 =====
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW _classification_raw AS
# MAGIC SELECT
# MAGIC   p.id,
# MAGIC   p.positive_point,
# MAGIC   p.negative_feedback,
# MAGIC   -- ポジティブコメントの分類
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-120b',  -- ←ご自身のエンドポイント名に変更
# MAGIC     p.prompt_for_positive,
# MAGIC     responseFormat => 'STRUCT<
# MAGIC       classification:STRUCT<
# MAGIC         category: STRING,
# MAGIC         confidence: DOUBLE
# MAGIC       >
# MAGIC     >'
# MAGIC   ) AS positive_classification,
# MAGIC   -- ネガティブコメントの分類
# MAGIC   ai_query(
# MAGIC     'databricks-gpt-oss-120b',  -- ←ご自身のエンドポイント名に変更
# MAGIC     p.prompt_for_negative,
# MAGIC     responseFormat => 'STRUCT<
# MAGIC       classification:STRUCT<
# MAGIC         category: STRING,
# MAGIC         confidence: DOUBLE
# MAGIC       >
# MAGIC     >'
# MAGIC   ) AS negative_classification
# MAGIC FROM _classification_prompt p;
# MAGIC
# MAGIC -- 分類結果の確認
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   positive_point,
# MAGIC   positive_classification
# MAGIC FROM _classification_raw
# MAGIC WHERE positive_point IS NOT NULL
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ6-5: 最終的なGoldテーブルの作成
# MAGIC
# MAGIC すべての情報を統合して、最終的なGoldテーブルを作成します。
# MAGIC
# MAGIC #### 含まれる情報
# MAGIC
# MAGIC 1. **基本情報**: ID、提出日時、属性情報
# MAGIC 2. **アンケート回答**: 飲酒頻度、満足度など
# MAGIC 3. **AI抽出情報**: ポジティブ/ネガティブコメント、シーン
# MAGIC 4. **AI分類情報**: カテゴリと信頼度

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== ステップ5: 最終的なGoldテーブルの作成 =====
# MAGIC
# MAGIC CREATE OR REPLACE TABLE gold_survey_responses_final AS
# MAGIC SELECT
# MAGIC   -- 基本情報
# MAGIC   b.id,
# MAGIC   b.submitted_at,
# MAGIC   b.age_group,
# MAGIC   b.gender,
# MAGIC   b.occupation,
# MAGIC   b.region,
# MAGIC   b.drives_weekly,
# MAGIC   
# MAGIC   -- アンケート回答
# MAGIC   b.drinking_freq,
# MAGIC   b.fav_alcohols_arr,
# MAGIC   b.nonalc_freq,
# MAGIC   b.health_reasons_arr,
# MAGIC   b.purchase_intent,
# MAGIC   b.price_band,
# MAGIC   b.satisfaction_int,
# MAGIC   b.free_comment,
# MAGIC   
# MAGIC   -- AI抽出情報
# MAGIC   n.positive_point,
# MAGIC   n.negative_feedback,
# MAGIC   n.scene,
# MAGIC   
# MAGIC   -- AI分類情報（ポジティブ）
# MAGIC   c.positive_classification:classification:category AS positive_point_category,
# MAGIC   c.positive_classification:classification:confidence AS positive_point_confidence,
# MAGIC   
# MAGIC   -- AI分類情報（ネガティブ）
# MAGIC   c.negative_classification:classification:category AS negative_feedback_category,
# MAGIC   c.negative_classification:classification:confidence AS negative_feedback_confidence
# MAGIC   
# MAGIC FROM silver_survey_responses_base b
# MAGIC LEFT JOIN gold_survey_responses_nlp n ON b.id = n.id
# MAGIC LEFT JOIN _classification_raw c ON b.id = c.id;
# MAGIC
# MAGIC -- 作成されたテーブルの確認
# MAGIC SELECT COUNT(*) AS total_records
# MAGIC FROM gold_survey_responses_final;

# COMMAND ----------

# MAGIC %md
# MAGIC ### ステップ6-6: テーブルのメタデータ設定
# MAGIC
# MAGIC テーブルと各列に説明を追加して、データの意味を明確にします。
# MAGIC
# MAGIC #### なぜメタデータが重要か
# MAGIC
# MAGIC - **ドキュメント化**: コードを見なくても列の意味がわかる
# MAGIC - **データガバナンス**: データの定義を一元管理
# MAGIC - **チーム協業**: 他のメンバーが理解しやすい
# MAGIC - **AI活用**: AIがメタデータを参照して適切なクエリを生成

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== テーブル全体のコメント =====
# MAGIC COMMENT ON TABLE gold_survey_responses_final IS '
# MAGIC アンケート回答データの最終版。
# MAGIC AI分析により、自由記述から情報抽出と自動分類を実施済み。
# MAGIC ビジネス分析やレポート作成に直接使用可能。';
# MAGIC
# MAGIC -- ===== 各列のコメント =====
# MAGIC
# MAGIC -- 基本情報
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.id IS '回答一意識別子（主キー）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.submitted_at IS '回答提出日時';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.age_group IS '年代グループ（u19, 20s, 30s, 40s, 50s, 60plus）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.gender IS '性別（male, female, other, no_answer）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.occupation IS '職業カテゴリ';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.region IS '地域カテゴリ';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.drives_weekly IS '週1回以上運転するか（Boolean）';
# MAGIC
# MAGIC -- アンケート回答
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.drinking_freq IS '普段の飲酒頻度（daily, weekly, monthly, never）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.fav_alcohols_arr IS '通常好む酒類（配列）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.nonalc_freq IS 'ノンアルコール飲用頻度（first_time, sometimes, weekly）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.health_reasons_arr IS '健康理由（配列）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.purchase_intent IS '購入意向（def_buy, maybe, unsure, no）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.price_band IS '許容価格帯';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.satisfaction_int IS '満足度（1-5の整数）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.free_comment IS '自由記述コメント（元データ）';
# MAGIC
# MAGIC -- AI抽出情報
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.positive_point IS 'AIが抽出したポジティブな意見';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.negative_feedback IS 'AIが抽出したネガティブな意見・要望';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.scene IS 'AIが抽出した飲用シーン';
# MAGIC
# MAGIC -- AI分類情報
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.positive_point_category IS '
# MAGIC ポジティブコメントの分類カテゴリ（18種類）：
# MAGIC 【領域1: Sensory Quality】
# MAGIC - Appearance: 外観・見た目
# MAGIC - Aroma: 香り
# MAGIC - Flavor: 味
# MAGIC - MouthfeelCarbonation: 口当たり・炭酸
# MAGIC - DrinkabilityAftertaste: 飲みやすさ・後味
# MAGIC 【領域2: Functional Health & Safety】
# MAGIC - ABV: アルコール度数
# MAGIC - CaloriesCarbs: カロリー・糖質
# MAGIC - AdditivesAllergen: 添加物・アレルゲン
# MAGIC - Ingredients: 原材料
# MAGIC 【領域3: Practical Usability】
# MAGIC - PackagingDesign: パッケージデザイン
# MAGIC - PackServingFormat: 容量・形態
# MAGIC - ShelfLifeStorage: 保存性
# MAGIC 【領域4: Economic & Availability】
# MAGIC - PriceValue: 価格・コスパ
# MAGIC - Availability: 入手しやすさ
# MAGIC - PromotionVisibility: 販促・広告
# MAGIC 【領域5: Brand & Emotional Perception】
# MAGIC - BrandImageStory: ブランドイメージ
# MAGIC - SocialAcceptability: 社会的受容性
# MAGIC - SustainabilityEthics: 持続可能性・倫理';
# MAGIC
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.positive_point_confidence IS 'ポジティブコメント分類の信頼度（0.0-1.0）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.negative_feedback_category IS 'ネガティブコメントの分類カテゴリ（positive_point_categoryと同じ18種類）';
# MAGIC COMMENT ON COLUMN gold_survey_responses_final.negative_feedback_confidence IS 'ネガティブコメント分類の信頼度（0.0-1.0）';

# COMMAND ----------

# MAGIC %md
# MAGIC ### Goldテーブルの確認

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの全体像を確認
# MAGIC SELECT 
# MAGIC   id,
# MAGIC   age_group,
# MAGIC   gender,
# MAGIC   satisfaction_int,
# MAGIC   LEFT(free_comment, 30) AS comment_preview,
# MAGIC   positive_point,
# MAGIC   positive_point_category,
# MAGIC   ROUND(positive_point_confidence, 2) AS pos_conf,
# MAGIC   negative_feedback,
# MAGIC   negative_feedback_category,
# MAGIC   ROUND(negative_feedback_confidence, 2) AS neg_conf,
# MAGIC   scene
# MAGIC FROM gold_survey_responses_final
# MAGIC WHERE positive_point IS NOT NULL
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ### テーブルのスキーマ確認

# COMMAND ----------

# MAGIC %sql
# MAGIC -- テーブルの詳細情報（列名、型、コメント）を表示
# MAGIC DESCRIBE EXTENDED gold_survey_responses_final;

# COMMAND ----------

# MAGIC %md
# MAGIC ## ステップ7: データ品質の確認
# MAGIC
# MAGIC 作成したデータの品質を確認します。
# MAGIC
# MAGIC ### 確認項目
# MAGIC
# MAGIC 1. **データ件数**: 各層でのレコード数
# MAGIC 2. **NULL値の割合**: 重要な列のNULL率
# MAGIC 3. **AI分類の信頼度**: 分類結果の品質
# MAGIC 4. **カテゴリ分布**: 各カテゴリの出現頻度

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ===== データ品質レポート =====
# MAGIC
# MAGIC -- 1. 各層のレコード数
# MAGIC SELECT 
# MAGIC   'Bronze層' AS layer,
# MAGIC   COUNT(*) AS record_count
# MAGIC FROM bronze_survey_responses
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Silver層' AS layer,
# MAGIC   COUNT(*) AS record_count
# MAGIC FROM silver_survey_responses_base
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT 
# MAGIC   'Gold層' AS layer,
# MAGIC   COUNT(*) AS record_count
# MAGIC FROM gold_survey_responses_final;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. AI抽出の成功率
# MAGIC SELECT
# MAGIC   COUNT(*) AS total_records,
# MAGIC   COUNT(free_comment) AS has_comment,
# MAGIC   COUNT(positive_point) AS has_positive,
# MAGIC   COUNT(negative_feedback) AS has_negative,
# MAGIC   COUNT(scene) AS has_scene,
# MAGIC   ROUND(COUNT(positive_point) * 100.0 / COUNT(free_comment), 1) AS positive_extraction_rate,
# MAGIC   ROUND(COUNT(negative_feedback) * 100.0 / COUNT(free_comment), 1) AS negative_extraction_rate,
# MAGIC   ROUND(COUNT(scene) * 100.0 / COUNT(free_comment), 1) AS scene_extraction_rate
# MAGIC FROM gold_survey_responses_final
# MAGIC WHERE free_comment IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. AI分類の信頼度分布
# MAGIC SELECT
# MAGIC   'ポジティブ' AS comment_type,
# MAGIC   COUNT(*) AS total,
# MAGIC   ROUND(AVG(positive_point_confidence), 3) AS avg_confidence,
# MAGIC   ROUND(MIN(positive_point_confidence), 3) AS min_confidence,
# MAGIC   ROUND(MAX(positive_point_confidence), 3) AS max_confidence,
# MAGIC   COUNT(CASE WHEN positive_point_confidence >= 0.8 THEN 1 END) AS high_confidence_count,
# MAGIC   ROUND(COUNT(CASE WHEN positive_point_confidence >= 0.8 THEN 1 END) * 100.0 / COUNT(*), 1) AS high_confidence_rate
# MAGIC FROM gold_survey_responses_final
# MAGIC WHERE positive_point IS NOT NULL
# MAGIC
# MAGIC UNION ALL
# MAGIC
# MAGIC SELECT
# MAGIC   'ネガティブ' AS comment_type,
# MAGIC   COUNT(*) AS total,
# MAGIC   ROUND(AVG(negative_feedback_confidence), 3) AS avg_confidence,
# MAGIC   ROUND(MIN(negative_feedback_confidence), 3) AS min_confidence,
# MAGIC   ROUND(MAX(negative_feedback_confidence), 3) AS max_confidence,
# MAGIC   COUNT(CASE WHEN negative_feedback_confidence >= 0.8 THEN 1 END) AS high_confidence_count,
# MAGIC   ROUND(COUNT(CASE WHEN negative_feedback_confidence >= 0.8 THEN 1 END) * 100.0 / COUNT(*), 1) AS high_confidence_rate
# MAGIC FROM gold_survey_responses_final
# MAGIC WHERE negative_feedback IS NOT NULL;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. カテゴリ分布（ポジティブコメント）
# MAGIC SELECT
# MAGIC   positive_point_category AS category,
# MAGIC   COUNT(*) AS count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS percentage,
# MAGIC   ROUND(AVG(positive_point_confidence), 3) AS avg_confidence
# MAGIC FROM gold_survey_responses_final
# MAGIC WHERE positive_point_category IS NOT NULL
# MAGIC GROUP BY positive_point_category
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5. カテゴリ分布（ネガティブコメント）
# MAGIC SELECT
# MAGIC   negative_feedback_category AS category,
# MAGIC   COUNT(*) AS count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS percentage,
# MAGIC   ROUND(AVG(negative_feedback_confidence), 3) AS avg_confidence
# MAGIC FROM gold_survey_responses_final
# MAGIC WHERE negative_feedback_category IS NOT NULL
# MAGIC GROUP BY negative_feedback_category
# MAGIC ORDER BY count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## まとめ：ハンズオンの振り返り
# MAGIC
# MAGIC ### 学んだこと
# MAGIC
# MAGIC このハンズオンで、以下のスキルを習得しました：
# MAGIC
# MAGIC #### 1. メダリオンアーキテクチャの実装
# MAGIC
# MAGIC ✓ **Bronze層**: 生データの保存
# MAGIC - JSON Linesの読み込み
# MAGIC - 基本的な型変換
# MAGIC - Deltaテーブルへの保存
# MAGIC
# MAGIC ✓ **Silver層**: データのクリーニングと正規化
# MAGIC - カテゴリ値の正規化
# MAGIC - 配列型への変換
# MAGIC - 重複データの除去
# MAGIC
# MAGIC ✓ **Gold層**: ビジネス用途のデータ作成
# MAGIC - AI による情報抽出
# MAGIC - AI による自動分類
# MAGIC - メタデータの設定
# MAGIC
# MAGIC #### 2. Unity Catalogの活用
# MAGIC
# MAGIC ✓ Catalog、Schema、Volumeの作成と管理
# MAGIC ✓ テーブルへのコメント追加
# MAGIC ✓ データガバナンスの基礎
# MAGIC
# MAGIC #### 3. Databricks AI Functionsの活用
# MAGIC
# MAGIC ✓ ai_query()関数の使い方
# MAGIC ✓ プロンプトエンジニアリング
# MAGIC ✓ 構造化出力の取得
# MAGIC ✓ 大量データへのAI適用
# MAGIC
# MAGIC #### 4. データ品質管理
# MAGIC
# MAGIC ✓ データ品質の確認方法
# MAGIC ✓ AI分類の信頼度評価
# MAGIC ✓ 分析用のクエリ作成
# MAGIC

# COMMAND ----------


