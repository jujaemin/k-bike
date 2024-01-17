## 프로젝트 주제

기상 정보에 따른 서울시 자전거 대여 현황을 파악할 수 있도록 데이터 파이프라인을 구성하고 이를 시각화

## 주제 선정이유 및 기대효과

적은 전처리와 Update 주기가 빠른 데이터를 사용하여 ETL 파이프라인 구축 경험을 위함

이 데이터들의 파이프라인을 통한 시각화로, 날씨정보와 서울시 공공자전거 현황을 함께 확인하여 사용자들의 편리한 이용에 도움을 줄 수 있음

## 프로젝트 역할분담

| 이름 | 역할 |
| --- | --- |
| 장태수 | 워크플로 관리, DatawareHouse 구축  |
| 김형인 | ETL관리, 대시보드  |
| 유승상 | AWS구축, ELT관리 |
| 주재민 | ETL관리, 대시보드  |
| 최아정 | ETL관리, 대시보드  |


## 프로젝트 아키텍처

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/86a97404-d256-4b17-9c76-f5a171275460/Untitled.png)

- ec2
    - Docker로 Airflow, Superset container 실행하기 위함
    - 사양 - **Instance type** : **t3a.xlarge**(4 vCPU, 16GiB)

- airflow
    - 개인 local에서 테스트 후 최종 DAG 사용

- snowflake
    - redshift 비용문제로 snowflake 30일 무료제공 계정 사용
    - 분석용 데이터(data warehouse), raw데이터(data lake) 모두 적재
 
## 프로젝트 진행과정

수집에 사용한 api

https://data.seoul.go.kr/dataList/OA-21285/F/1/datasetView.do

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/5e33f05d-2a64-43e3-ac68-59e6e8328b36/Untitled.png)

- 따릉이 관련 데이터 수집
    - 따릉이 대여소 명
    - 따릉이 대여소 ID
    - 따릉이 주차건수
    - 따릉이 거치대수
    - 따릉이 거치율

- 날씨 관련 데이터 수집
    - 온도
    - 체감온도
    - 강수확률
    - 강수량
    - 자외선 지수 단계
    - 미세먼지농도
    - 초미세먼지농도

### ETL 구성

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/3bdecfba-1bbb-423e-bb8e-5a3d64efe959/Untitled.png)

Api 자체 문제로 반환값이 없는 경우가 발생

⇒값이 없는경우 Task를 미리 실패하도록 예외처리

```python
if not records:
		raise Exception('recodrds is empty')
```

### ELT 구성

분석용 데이터 data warehouse에 적재

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/3e4c813d-b58e-4a40-aa04-dd9d8880ec99/Untitled.png)
![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/f04415cc-5aee-4e76-aab6-6df3188e2f71/Untitled.png)


### 시각화 대시보드 구성

- 현재 기온, 강수확률, 평균 따릉이 거치율을 표시

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/eccc074f-5fa6-4f62-bbab-44ed61652bda/Untitled.png)

- 서울 주요지역별 구체적인 기상정보 확인가능

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/99aad517-5b33-4dfc-883d-59dc25e30a6e/Untitled.png)

- 장소별 따릉이 거치소의 구체적 현황 확인가능

![Untitled](https://prod-files-secure.s3.us-west-2.amazonaws.com/e937a7f9-dece-4540-8e1e-3c5966896424/ab7d8565-4a95-4d83-a138-55c5f6220a03/Untitled.png)







