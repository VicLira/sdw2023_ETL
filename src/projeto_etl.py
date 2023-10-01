import pandas as pd
import json
import openai
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# EXTRACT - ETL

sdw2023_csv_path = '../../projetoETL/resources/users.csv'
openai_api_key = load_dotenv.OPENAI_API_KEY

df = pd.read_csv(sdw2023_csv_path)
user_ids = df['id'].tolist()
print(user_ids)

def get_user(df, id):
    user = df[df['id'] == id]
    return user.to_dict(orient='records')[0] if not user.empty else None

users = [user for id in user_ids if (user := get_user(df, id)) is not None]
print(json.dumps(users, indent=2))

# TRANSFORM - ETL

openai.api_key = openai_api_key

def generate_ai_news(user):
    completion = openai.ChatCompletion.create(
        model="gpt-3.5-turbo",
        max_token=60,
        messages=[
            {
                "role": "system",
                "content": "Você é um especialista em Sustentabilidade."
            },
            {
                "role": "user",
                "content": f"Crie uma mensagem para {user['name']} sobre a importância de economizar energia (máximo de 100 caracteres)"
            }
        ]
    )
    return completion.choices[0].message.content.strip('\"')

# Adicionar um novo campo "data" ao CSV a cada nova notícia
for user in users:
    news = generate_ai_news(user)
    current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    new_data = {
        "icon": "https://digitalinnovationone.github.io/santander-dev-week-2023-api/icons/credit.svg",
        "description": news,
        "data": current_date  # Adicionar a data atual
    }
    if 'news' not in user:
        user['news'] = []  # Se o campo 'news' não existir, crie uma lista vazia
    user['news'].append(new_data)

# Atualizar o arquivo CSV com os novos dados adicionados
df.to_csv(sdw2023_csv_path, index=False)

# LOAD - ETL
print("Dados atualizados no arquivo CSV.")
