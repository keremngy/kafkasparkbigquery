
import streamlit as st
#google connectorlerini import ettims
from google.oauth2 import service_account
from google.cloud import bigquery
import pandas as pd
#grafikleri plotly ile oluşturdum
import plotly.express as px 
#piechart için graph objects import ettim
import plotly.graph_objects as go 
#sleep kullanmak için import ettim
import time
#numpy select kullandım
import numpy as np


# streamlit layout ayarları
st.set_page_config(
    page_title="NYC Kopek Saldirilari",
    layout="wide",
    initial_sidebar_state="auto",
)


# api clientin oluşturulması
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)


#bu kısmı dökümantasyondan alarak projeme uyguladım
# clearcache ve rerun içinde bir bekleme süresi oluşturdu

@st.experimental_memo(ttl=10)
def run_query(sql):
    job_config = bigquery.QueryJobConfig(use_query_cache=False)
    query_job = client.query(sql, job_config=job_config)
    rows= query_job.to_dataframe()
    return rows

#sorgudan bir df oluşturuyorum
df=run_query("SELECT * FROM `keremkafka.kopeksaldiri`")

#rerun için manual bir buton ekledim
if st.button("Reload"):

    st.experimental_rerun()


#sütun adlarını yeniden isimlendirdim
df = df.rename(columns={"dfalias_tarih_":"Tarih",
                        "dfalias_bolge_":"Bolge",
                        "dfalias_cins_":"Cins",
                        "count" : "Vaka Sayisi"
                       })

#tarih string geliyordu date'e çevirdim ve diğer alanlar içinde data typeları tanımladım
df['Tarih'] = pd.to_datetime(df['Tarih']).dt.date

df= df.astype({
    "Bolge":str,
    "Cins":str,
    "Vaka Sayisi":int
})

#tarihten ay bilgisini alıp ayları yazdıracağım
aynumarasi = df['Ayno'] = pd.DatetimeIndex(df['Tarih']).month

#aylari isimlendiriyorum
durumlar = [
    (aynumarasi == 1),
    (aynumarasi == 2),
    (aynumarasi == 3),
    (aynumarasi == 4),
    (aynumarasi == 5),
    (aynumarasi == 6),
    (aynumarasi == 7),
    (aynumarasi == 8),
    (aynumarasi == 9),
    (aynumarasi == 10),
    (aynumarasi == 11),
    (aynumarasi == 12),
           ]

ayadlari = ['Ocak','Subat','Mart','Nisan','Mayis','Haziran','Temmuz','Agustos','Eyluk','Ekim','Kasim','Aralik']

#numpy select ile durumlar ve ay adlarini tanımladım
df['Aylar'] = np.select(durumlar,ayadlari)


####### Analizler buradan başlıyor #######
#sayfa başlığı ve açıklaması yazdım
st.title('NYC Köpek Saldırıları')
st.markdown("""Bolgelere ve cinslerine gore kopek saldirilari durumu""")
st.text("")

placeholder = st.empty()

#dökümantasyona göre grafiklerin interkativ olması için for ve while içerisinde 
# yazılması önerilmiş bu şekilde 30snde bir yenileyecek

for seconds in range(30):

### Toplam Analizleri
#df den bolge ve cins alanlarının sayılarını alıyorum

    bolgesayisi= df["Bolge"].nunique()
    cinssayisi= df["Cins"].nunique()

#tekilsayi olarak yazdırıyorum
    with placeholder.container():
        bolge, cins, toplamv = st.columns(3)

        bolge.metric(
            label="Bölge Sayisi",
            value= bolgesayisi
        )
        toplamv.metric(
            label="Toplam Vaka",
            value= df['Vaka Sayisi'].sum()
        )
        cins.metric(
            label="Cins Sayisi",
            value= cinssayisi
        )

        #toplamlar ile grafikler arasında bosluk bırakmak için kullandım
        st.markdown("")

### Grafikler ###

# Bolgelere göre toplam saldırılar. Bolgeye gore gruplandı ve azalan şekilde sıralandı ardından ilk 10 değer alındı.
#bolgedf
        toplambolge=df.groupby(['Bolge'])[['Vaka Sayisi']].sum().reset_index()
        toplambolgesort = toplambolge=toplambolge.sort_values(by=['Vaka Sayisi'],ascending=False)
        toplambolgedf = toplambolgesort.head(10)

# Cinslere göre toplam saldırılar(Cinslere gore gruplandı ve azalan şekilde sıralandı ardından ilk 10 değer alındı)
# Bu degiskenleri ilk grafikte kullanacağım
        bolgelabel = toplambolgedf['Bolge']
        bolgevalues = toplambolgedf['Vaka Sayisi']


# Cinslere göre toplam saldırılar(Cinslere gore gruplandı ve azalan şekilde sıralandı ardından ilk 10 değer alındı)
        toplamcins=df.groupby(['Cins'])[['Vaka Sayisi']].sum().reset_index()
        toplamcinssort=toplamcins.sort_values(by=['Vaka Sayisi'],ascending=False)
        toplamcinsdf = toplamcinssort.head(10)

#Bölge ve Cinsler beraber gruplandı ilk grafikte kullanacağım
        toplambolcins=df.groupby(['Bolge','Cins'])[['Vaka Sayisi']].sum().reset_index()
        toplambolcinssort=toplambolcins.sort_values(by=['Vaka Sayisi'],ascending=False)
        toplambolcinsdf = toplambolcinssort.head(10)
        toplambolcinsf = toplambolcinssort[toplambolcinssort['Vaka Sayisi'] > toplambolcinssort['Vaka Sayisi'].mean()]

#Ay adlarına göre gruplayarak toplam aldıracağım bunu en alttaki detay tablodardan birinde göstereceğim
        toplamaylar=df.groupby(['Aylar'])[['Vaka Sayisi']].sum().reset_index()
        toplamaylarsort =toplamaylar.sort_values(by=['Vaka Sayisi'],ascending=False)


    # Toplam Bolge , Cins grafikleri column sayısını belirtips with ile yazıyoruz
        toplambc, toplamb,toplamc = st.columns(3)

        with toplambc:
            st.markdown("Bölgelerde ortalamanın üzerinde saldırıda bulunan cinsler")
            fig3 = px.treemap(data_frame=toplambolcinsf,
                             path=[px.Constant(''), 'Bolge', 'Cins'],
                             values='Vaka Sayisi',
                             width=900,
                             height=500)
            st.write(fig3)

        with toplamb:
            st.markdown("Bölgelere göre toplam köpek saldırıları")
            fig = go.Figure(data=[go.Pie(labels=bolgelabel, values=bolgevalues, hole=.3)
                                  ])
            fig.update_layout(margin=dict(t=5, b=4, l=4, r=4))
            st.write(fig)

        with toplamc:
            st.markdown("Cinslere göre toplam köpek saldırıları ilk 10")
            fig2 = px.bar(
                data_frame=toplamcinsdf, x="Cins",y="Vaka Sayisi",
                barmode='group',
                width=900,
                height=500
            )
            st.write(fig2)


    ### dataframeleri detay tablo olarak yazdıracağım ancak burada df yazdırdığım zaman satır numaraları gözükmemesi 
    # için şu kodu uyguluyorum
        # CSS to inject contained in a string
        hide_table_row_index = """
                    <style>
                    thead tr th:first-child {display:none}
                    tbody th {display:none}
                    </style>
                    """
        # Inject CSS with Markdown
        st.markdown(hide_table_row_index, unsafe_allow_html=True)

    ### DETAY GÖRÜNÜMLER adı altında yukarıda oluşturduğum df'leri tablo olarak yazdırdım

        st.title("Detay Görünümler")
        st.markdown("")
        aylartoplam, toplambdetay, toplamcinsdetay = st.columns(3)

        with aylartoplam:
            st.markdown("Aylara Gore Toplam Saldırılar")
            st.table(toplamaylarsort)
        with toplambdetay:
            st.markdown("Bölgelere Gore Toplam Saldirilar")
            st.table(toplambolgedf)
        with toplamcinsdetay:
            st.markdown("Cinslere Gore Toplam Saldirilar")
            st.table(toplamcinsdf)

        time.sleep(1)

#scripti otomatik olarak yenilemek için cache silerek rerun yapıyorum
def clearcache():
    st.experimental_memo.clear()
clearcache()
time.sleep(2)
def rerun():
    st.experimental_rerun()
rerun()
