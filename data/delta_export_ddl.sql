CREATE TABLE handson.survey_analysis.gold_survey_responses_final (
  id BIGINT COMMENT '回答一意識別子 (primary key)；feedback テーブルとの参照キー',
  submitted_at TIMESTAMP COMMENT '回答提出日時（タイムスタンプ）',
  age_group STRING COMMENT '年代グループ（例: u19, 20s, 30s, 40s, 50s, 60plus）',
  gender STRING COMMENT '性別カテゴリ：male, female, other, no_answer',
  occupation STRING COMMENT '職業カテゴリ（例: company_employee, student, homemaker, other）',
  region STRING COMMENT '地域カテゴリ（hokkaido_to_hoku, kanto, chubu, kinki, chugoku_shikoku, kyushu_okinawa, overseas, no_answer）',
  drives_weekly BOOLEAN COMMENT '週1 回以上運転するかどうか（真偽値）',
  drinking_freq STRING COMMENT '普段の飲酒頻度：daily, weekly, monthly, never',
  fav_alcohols_arr ARRAY<STRING> COMMENT '通常好む酒類（文字列、複数可）（ARRAY<STRING>）',
  nonalc_freq STRING COMMENT 'ノンアル飲用頻度：first_time, sometimes, weekly',
  health_reasons_arr ARRAY<STRING> COMMENT 'health_reasons を配列化したもの（ARRAY<STRING>）',
  purchase_intent STRING COMMENT '購入意向カテゴリ：def_buy, maybe, unsure, no',
  price_band STRING COMMENT '許容価格帯カテゴリ（例: under199, 200_249, 250_299, 300_349, 350plus）',
  satisfaction_int INT COMMENT '満足度を整数型に変換したもの（1～5 の INT）',
  free_comment STRING COMMENT '自由記述による感想文',
  positive_point STRING COMMENT 'AI 抽出によるポジティブ文句群（ARRAY<STRING>）',
  negative_feedback STRING COMMENT 'AI 抽出によるネガティブ・改善要望文句群（ARRAY<STRING>）',
  scene STRING COMMENT '飲用シーン語句（ARRAY<STRING>）',
  positive_point_category STRING COMMENT '
positive_pointの分類カテゴリ：以下 18 種類のいずれか。  
1. Appearance：外観・見た目（色・透明度・泡質）  
2. Aroma：香り（ホップ香・モルト香・果実香・オフ香）  
3. Flavor：味覚（甘味・苦味・酸味・コク・バランス）  
4. MouthfeelCarbonation：口当たり・炭酸感（泡・ガス強度・舌触り）  
5. DrinkabilityAftertaste：飲みやすさ・後味（ゴクゴク・キレ・残留感）  
6. ABV：アルコール度数への言及（例: “0.00%”）  
7. CaloriesCarbs：カロリー・糖質関係の言及  
8. AdditivesAllergen：添加物・香料・アレルゲンの言及  
9. Ingredients：原材料・素材・産地・自然由来  
10. PackagingDesign：包装・ラベル・外装デザイン  
11. PackServingFormat：容量・パッケージ形態（缶・瓶・パック数）  
12. ShelfLifeStorage：保存性・賞味期限・開封後品質  
13. PriceValue：価格・コスパ・値ごろ感  
14. Availability：流通性・店舗・EC 販売可能性  
15. PromotionVisibility：販促・広告・POP・訴求力  
16. BrandImageStory：ブランドイメージ・理念・物語性  
17. SocialAcceptability：運転中・妊娠中などでも飲める許容感  
18. SustainabilityEthics：環境配慮・サステナビリティ・倫理性',
  positive_point_confidence STRING COMMENT 'positive_point_category に対するConfidence Score。分類信頼度（0.0～1.0）',
  negative_feedback_category STRING COMMENT '
negative_feedbackの分類カテゴリ：以下 18 種類のいずれか。  
1. Appearance：外観・見た目（色・透明度・泡質）  
2. Aroma：香り（ホップ香・モルト香・果実香・オフ香）  
3. Flavor：味覚（甘味・苦味・酸味・コク・バランス）  
4. MouthfeelCarbonation：口当たり・炭酸感（泡・ガス強度・舌触り）  
5. DrinkabilityAftertaste：飲みやすさ・後味（ゴクゴク・キレ・残留感）  
6. ABV：アルコール度数への言及（例: “0.00%”）  
7. CaloriesCarbs：カロリー・糖質関係の言及  
8. AdditivesAllergen：添加物・香料・アレルゲンの言及  
9. Ingredients：原材料・素材・産地・自然由来  
10. PackagingDesign：包装・ラベル・外装デザイン  
11. PackServingFormat：容量・パッケージ形態（缶・瓶・パック数）  
12. ShelfLifeStorage：保存性・賞味期限・開封後品質  
13. PriceValue：価格・コスパ・値ごろ感  
14. Availability：流通性・店舗・EC 販売可能性  
15. PromotionVisibility：販促・広告・POP・訴求力  
16. BrandImageStory：ブランドイメージ・理念・物語性  
17. SocialAcceptability：運転中・妊娠中などでも飲める許容感  
18. SustainabilityEthics：環境配慮・サステナビリティ・倫理性',
  negative_feedback_confidence STRING COMMENT 'negative_feedback_category に対するConfidence Score。分類信頼度（0.0～1.0）')
USING delta
COMMENT '
アンケート回答データ。加えて、さまざまなデータ加工を行い、いくつかの分析用のデータを追加したもの。'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7')
