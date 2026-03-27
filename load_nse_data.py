import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# ================= DB CONNECTION =================
conn = psycopg2.connect(
    "postgresql://neondb_owner:npg_IFJasBq3XS0D@ep-cool-bar-a1laxmw2-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
)

cur = conn.cursor()

# ================= NSE TICKERS =================
tickers = [
'ABBOTINDIA.NS','ADANIENT.NS','AHLEAST.NS','ALKEM.NS','AMARAJABAT.NS',
'APLAPOLLO.NS','APOLLOHOSP.NS','ASTRAZEN.NS','AUBANK.NS','AUROPHARMA.NS',
'AXISBANK.NS','BAJAJ-AUTO.NS','BAJAJFINSV.NS','BAJAJHLDNG.NS','BALKRISHNA.NS',
'BANCOINDIA.NS','BANKBARODA.NS','BANKINDIA.NS','BATAINDIA.NS','BHARATFORG.NS',
'BIOCON.NS','BLUESTARCO.NS','BOSCHLTD.NS','BRIGADE.NS','BRITANNIA.NS',
'CADILAHC.NS','CANBK.NS','CAPLIPOINT.NS','CHOLAFIN.NS','CIGNITITEC.NS',
'CIPLA.NS','CROMPTON.NS','CYIENT.NS','DAAWAT.NS','DATAMATICS.NS',
'DCAL.NS','DIVISLAB.NS','DIXON.NS','DLF.NS','DREDGECORP.NS',
'EICHERMOT.NS','EMAMILTD.NS','EXIDEIND.NS','FACT.NS','FINPIPE.NS',
'GAIL.NS','GLAXO.NS','GLENMARK.NS','GODREJCP.NS','GODREJPROP.NS',
'GREENPOWER.NS','GSPL.NS','GULFOILLUB.NS','HAVELLS.NS','HCC.NS',
'HCG.NS','HCLTECH.NS','HDFC.NS','HEIDELBERG.NS','HEROMOTOCO.NS',
'HUBTOWN.NS','ICICIBANK.NS','ICICIGI.NS','ICICIPRULI.NS','IDFC.NS',
'IEX.NS','IMFA.NS','INDUSINDBK.NS','INFIBEAM.NS','JINDALSAW.NS',
'JSWSTEEL.NS','JUBLFOOD.NS','JYOTISTRUC.NS','KOTAKBANK.NS','L&TFH.NS',
'LUPIN.NS','LYPSAGEMS.NS','MAGNUM.NS','MARICO.NS','MARUTI.NS',
'MAWANASUG.NS','METKORE.NS','MINDTREE.NS','MOIL.NS','MOTHERSUMI.NS',
'MPHASIS.NS','MRF.NS','MURUDCERA.NS','NESTLEIND.NS','NETWORK18.NS',
'NMDC.NS','OBEROIRLTY.NS','OFSS.NS','PDMJEPAPER.NS','PNB.NS',
'PVR.NS','QUICKHEAL.NS','RANEHOLDIN.NS','RBLBANK.NS','RELAXO.NS',
'RELIANCE.NS','SABTN.NS','SAGCEM.NS','SAIL.NS','SUNPHARMA.NS',
'SUNTECK.NS','TATACOMM.NS','TBZ.NS','TCPLPACK.NS','TECHM.NS',
'TIINDIA.NS','TIMKEN.NS','TV18BRDCST.NS','TVSMOTOR.NS','TVSSRICHAK.NS',
'UCOBANK.NS','UNIENTER.NS','VARDHACRLC.NS','VEDL.NS','VGUARD.NS',
'VOLTAS.NS','WELCORP.NS','WHIRLPOOL.NS','WIPRO.NS','YESBANK.NS','ZEEL.NS',
'5PAISA.NS','63MOONS.NS','20MICRONS.NS','3IINFOTECH.NS','3MINDIA.NS',
'A2ZINFRA.NS','AARTIDRUGS.NS','AARTIIND.NS','ABAN.NS','ABB.NS',
'ACC.NS','ACCELYA.NS','ACE.NS','ADANIPORTS.NS','ADANIPOWER.NS',
'ADANITRANS.NS','ADFFOODS.NS','ADORWELD.NS','ADVANIHOTR.NS','AEGISCHEM.NS',
'AGCNET.NS','AGRITECH.NS','AHLUCONT.NS','AIAENG.NS','AJANTPHARM.NS',
'AJMERA.NS','AKZOINDIA.NS','ALANKIT.NS','ALEMBICLTD.NS','ALICON.NS',
'ALKALI.NS','ALKYLAMINE.NS','ALLCARGO.NS','ALLSEC.NS','ALPHAGEO.NS',
'AMBUJACEM.NS','AMDIND.NS','AMRUTANJAN.NS','ANANTRAJ.NS','ANSALHSG.NS',
'ANSALAPI.NS','APARINDS.NS','APCOTEXIND.NS','APOLLOTYRE.NS','APTECHT.NS',
'ARCHIDPLY.NS','ARCHIES.NS','ARCOTECH.NS','AROGRANITE.NS','ASAHIINDIA.NS',
'ASAHISONG.NS','ASHAPURMIN.NS','ASHIANA.NS','ASHOKLEY.NS','ASHOKA.NS',
'ASIANTILES.NS','ASIANHOTNR.NS','ASIANPAINT.NS','ASTEC.NS','ASTRAMICRO.NS',
'ASTRAL.NS','ATLANTA.NS','ATULAUTO.NS','ATUL.NS','AURIONPRO.NS',
'AUSOMENT.NS','AUTOLITIND.NS','AUTOAXLES.NS','AVANTIFEED.NS','AVTNPL.NS',
'AXISCADES.NS','BAJAJELEC.NS','BAJFINANCE.NS','BAJAJHIND.NS','BALPHARMA.NS',
'BALAMINES.NS','BALAJITELE.NS','BALKRISIND.NS','BALLARPUR.NS','BALMLAWRIE.NS',
'BALRAMCHIN.NS','BANARISUG.NS','BASF.NS','BAYERCROP.NS','BEDMUTHA.NS',
'BEML.NS','BERGEPAINT.NS','BFINVEST.NS','BFUTILITIE.NS','BGRENERGY.NS',
'BHAGERIA.NS','BEL.NS','BHARATGEAR.NS','BHEL.NS','BPCL.NS',
'BHARTIARTL.NS','BIL.NS','BIRLACORPN.NS','BLISSGVS.NS','BLUEDART.NS',
'BODALCHEM.NS','BBTC.NS','BOMDYEING.NS','BPL.NS','BROOKS.NS',
'CANFINHOME.NS','CARBORUNIV.NS','CARERATING.NS','CASTROLIND.NS','CCL.NS',
'CEATLTD.NS','CENTRALBK.NS','CENTUM.NS','CENTURYPLY.NS','CENTURYTEX.NS',
'CERA.NS','CESC.NS','CHAMBLFERT.NS','CHENNPETRO.NS','CINELINE.NS',
'CUB.NS','COALINDIA.NS','COLPAL.NS','CONCOR.NS','COROMANDEL.NS',
'COSMOFILMS.NS','CRISIL.NS','CUMMINSIND.NS','CYBERTECH.NS','DBCORP.NS',
'DABUR.NS','DALMIASUG.NS','DCBBANK.NS','DCMSHRIRAM.NS','DEEPAKFERT.NS',
'DEEPAKNTR.NS','DELTACORP.NS','DHAMPURSUG.NS','DHANBANK.NS','DISHTV.NS'
]

# ================= SETTINGS =================
BATCH_SIZE = 25
start_date = "2022-01-01"

frames = []

# ================= DOWNLOAD =================
for i in range(0, len(tickers), BATCH_SIZE):
    batch = tickers[i:i+BATCH_SIZE]
    print(f"Downloading batch {i} to {i+len(batch)}")

    df = yf.download(batch, start=start_date, group_by='ticker', progress=False)

    for t in batch:
        try:
            temp = df[t]['Close'].reset_index()
            temp['ticker'] = t
            temp.columns = ['date','value','ticker']
            frames.append(temp)
        except:
            pass

# ================= COMBINE =================
final_df = pd.concat(frames, ignore_index=True)

# ================= CLEAN =================
final_df = final_df.dropna()
final_df['date'] = pd.to_datetime(final_df['date']).dt.date
final_df = final_df[['date','value','ticker']]

print("Download complete. Starting insert...")

# ================= BULK INSERT =================
data_tuples = list(final_df.itertuples(index=False, name=None))

execute_values(
    cur,
    """
    INSERT INTO sp500_prices (date, value, ticker)
    VALUES %s
    ON CONFLICT DO NOTHING
    """,
    data_tuples
)

conn.commit()
cur.close()
conn.close()

print("ALL DATA LOADED FAST 🚀")