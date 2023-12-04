import sqlite3
from sklearn.feature_extraction.text import TfidfVectorizer
import pymorphy3
from collections import Counter, OrderedDict
import re

conn = sqlite3.connect('lab.db')
c = conn.cursor()

# Означення функції для створення таблиць із різних текстів
def create_tables(text_number):
	c.execute(f"""CREATE TABLE IF NOT EXISTS Main_text{text_number}
		(word_id INTEGER PRIMARY KEY AUTOINCREMENT,
		word_form TEXT,
		lemma TEXT,
		part_of_speech TEXT,
		word_form_frequency INTEGER)""")

	c.execute(f"""CREATE TABLE IF NOT EXISTS WordFormsFreq_text{text_number}
		(word_id INTEGER PRIMARY KEY AUTOINCREMENT,
		word_form TEXT,
		word_form_frequency INTEGER)""")

	c.execute(f"""CREATE TABLE IF NOT EXISTS LemmasFreq_text{text_number}
		(lemma_id INTEGER PRIMARY KEY AUTOINCREMENT,
		lemma TEXT,
		lemma_frequency INTEGER)""")

	c.execute(f"""CREATE TABLE IF NOT EXISTS PosFreq_text{text_number}
		(pos_id INTEGER PRIMARY KEY AUTOINCREMENT,
		part_of_speech TEXT,
		pos_frequency INTEGER)""")

# Означення функції для вибору файлу та його токенізації
def text_choice(file_path):
	with open(rf"{file_path}", 'r', encoding = "utf-8") as file:
		global word_dict, word_list
		word_dict = {}
		word_list = []
		pattern = r"[ҐґЄєІіЇїА-Яа-я’'-]+"
		text_1 = file.read().lower()
		splitted_1 = re.findall(pattern, text_1)
		number_of_tokens = 10000
		for token in splitted_1:
			if number_of_tokens > 0:
				if token not in word_dict.keys():
					word_dict.update({token: 1})
					word_list.append(token)
					number_of_tokens -= 1
				else:
					word_list.append(token)
					word_dict[token] += 1
					number_of_tokens -= 1
			else: break

morph = pymorphy3.MorphAnalyzer(lang='uk')

# Означення функції для отримання та збереження даних із тексту, як список лем, частоту словоформ та лем, відповідність частині мови і т.д.
def create_info(dict):
	global lemmas
	word_forms, frequencies, word_ids, lemmas, pos = [], [], [], [], []
	id_number = 1
	for key, value in dict.items():
		word_ids.append(id_number)
		id_number += 1
		word_forms.append(key)
		frequencies.append(value)
		word = morph.parse(key)[0]
		lemmas.append(word.normal_form)
		pos.append(word.tag.POS)
	main_info = [tuple(word_ids), tuple(word_forms),
	tuple(lemmas), tuple(pos), tuple(frequencies)]
	word_forms_info = [tuple(word_ids), tuple(word_forms), tuple(frequencies)]

	lemmas, pos = [], []
	for token in word_list:
		word = morph.parse(token)[0]
		lemmas.append(word.normal_form)
		pos.append(word.tag.POS)

	lemma_counts = Counter(lemmas)
	lemma_info = [
		tuple(range(1, len(lemma_counts) + 1)),
		tuple(lemma_counts.keys()),
		tuple(lemma_counts.values())]

	pos_counts = Counter(pos)
	pos_info = [
		tuple(range(1, len(pos_counts) + 1)),
		tuple(pos_counts.keys()),
		tuple(pos_counts.values())]	

	return main_info, word_forms_info, lemma_info, pos_info

# Означення функції для збереження інформації в базі даних
def db_update(text_number, m_info, wf_info, l_info, p_info):
	transposed_info = list(zip(*m_info))
	c.executemany(f"""INSERT INTO Main_text{text_number} VALUES
		(?, ?, ?, ?, ?)""", transposed_info)
	transposed_info = list(zip(*wf_info))
	c.executemany(f"""INSERT INTO WordFormsFreq_text{text_number} VALUES
		(?, ?, ?)""", transposed_info)
	transposed_info = list(zip(*l_info))
	c.executemany(f"""INSERT INTO LemmasFreq_text{text_number} VALUES
		(?, ?, ?)""", transposed_info)
	transposed_info = list(zip(*p_info))
	c.executemany(f"""INSERT INTO PosFreq_text{text_number} VALUES
		(?, ?, ?)""", transposed_info)
	conn.commit()

# Означення функції для отримання TF-IDF та його збереження в базі даних
def tf_idf(text_number, lemmas):
	tfidf_vectorizer = TfidfVectorizer()
	tfidf_matrix = tfidf_vectorizer.fit_transform([' '.join(lemmas)])
	features_names = tfidf_vectorizer.get_feature_names_out()
	tfidf_scores = tfidf_matrix.toarray()[0]

	c.execute(f"""CREATE TABLE IF NOT EXISTS TF_IDF_text{text_number}
		(lemma_id INTEGER PRIMARY KEY AUTOINCREMENT,
		lemma TEXT,
		tf_idf REAL)""")

	tf_idf_list = []
	for i, a in enumerate(features_names):
		tf_idf_list.append((features_names[i], tfidf_scores[i] ))
	c.executemany(f"""INSERT INTO TF_IDF_text{text_number}
		(lemma, tf_idf) VALUES (?, ?)""", tf_idf_list)

	conn.commit()

# Запуск функцій
create_tables(1)
text_choice(r"rozumniy-druk-915a0-redacted.txt")
main_info, word_forms_info, lemma_info, pos_info = create_info(word_dict)
db_update(1, main_info, word_forms_info, lemma_info, pos_info)
tf_idf(1, lemmas)

create_tables(2)
text_choice(r"amulet-paskalia.txt")
main_info, word_forms_info, lemma_info, pos_info = create_info(word_dict)
db_update(2, main_info, word_forms_info, lemma_info, pos_info)
tf_idf(2, lemmas)

conn.close()