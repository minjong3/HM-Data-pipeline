# H&M 데이터 파이프라인 구축
---

- H&M 데이터 파이프라인 구축   <a href="https://github.com/yeardream-de-project-team11/project-team11">
이번 프로젝트의 코드는 위 링크에 Wiki에 정리해 두었습니다.
**프로젝트 기간 :** 2023.11.06 ~ 2023.12.15
**프로젝트 인원 :** 4명


### 프로젝트 개요

* 우리의 목표는 고객들이 H&M에서 보다 나은 쇼핑 경험을 할 수 있도록, 개인의 취향과 선호도에 맞춘 맞춤형 패션 아이템을 제안하는 것입니다. 이를 통해 H&M은 고객들의 다양한 니즈에 신속하게 대응하며, 판매 전략과 재고 관리를 최적화하여 경쟁력을 강화하고자 합니다.

### 사용된 스킬

- **언어**
  ![Static Badge](https://img.shields.io/badge/Python%20-%23003057)
- **AWS**
  ![Static Badge](https://img.shields.io/badge/S3%20-%23003057) ![Static Badge](https://img.shields.io/badge/EMR%20-%23003057) ![Static Badge](https://img.shields.io/badge/EC2%20-%23003057) ![Static Badge](https://img.shields.io/badge/Quicksight%20-%23003057) ![Static Badge](https://img.shields.io/badge/Athena%20-%23003057)
- **컨테이너화**
  ![Static Badge](https://img.shields.io/badge/Docker%20-%23003057)
- **모니터링 및 시각화**
  ![Static Badge](https://img.shields.io/badge/Grafana%20-%23003057) ![Static Badge](https://img.shields.io/badge/Prometheus%20-%23003057)
- **데이터 워크플로우 자동화**
  ![Static Badge](https://img.shields.io/badge/Apache%20Airflow%20-%23003057)
- **빅데이터 처리**
 ![Static Badge](https://img.shields.io/badge/Apache%20Spark%20-%23003057)
- **커뮤니티 도구**
  ![Static Badge](https://img.shields.io/badge/Slack%20-%23003057) ![Static Badge](https://img.shields.io/badge/Git%20hub%20-%23003057) ![Static Badge](https://img.shields.io/badge/Zoom%20-%23003057)

### 팀원 및 역할

| GitHub ID   | Roles                                                                                                                                                                                                 |
|--------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| @minjong3 | - S3에 파티셔닝 후 parquet 형식으로 저장- Airflow로 EMR클러스터 자동화 및 적합성 중복성 체크 자동화 후 슬랙 알람|
| @humaningansalam | - S3에 파티셔닝 후 parquet 형식으로 저장 - EMR 환경 구축 - mlflow - 모티터링  |
| @kclown0 | - 데이터 전처리 - superset 설치 및 시각화 |
| @bokusan | - 데이터 전처리 - quicksight, super set 시각화 - docker 및 airflow 설치|

### 프로젝트 구현 세부정보

1. 원본 데이터를 S3에 적재합니다.
2. EMR을 통해 원본 데이터를 파티셔닝 후 parquet 형식으로 저장합니다.
3. Athena를 통해 데이터를 분석 및 적합성 중복성을 체크합니다.
4. quicksight, Superset, Grafana, Prometheus, Slack 등으로 시각화 및 모니터링을 진행합니다.
5. Airflow를 통해 데이터 파이프라인을 자동화 시켜 모니터링 및 알림만 확인합니다.
   
![image](https://github.com/yeardream-de-project-team11/project-team11/assets/104144701/599d8a4a-4499-4121-a609-efc6966a3728)

### 프로젝트 리뷰

- 데이터 파이프라인을 안정적으로 구축하였으나, 추후 데이터를 더 이해하고 버킷팅이나 파티셔닝을 하도록 발전할 계획입니다.
- 머신러닝을 통해 고객 맞춤 제품 추천 서비스를 완성한다면 더욱 완성도 있는 프로젝트가 될 것 같습니다.

