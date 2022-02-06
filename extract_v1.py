import re
import os
import unicodedata
import pickle
from collections import defaultdict

import pandas as pd
from tqdm import tqdm



TOKEN_RGX = re.compile(r'\b[^\d\W]+\b')
def tokenize_regex(text, window=50):
    """Utility function to tokenize simple
    """
    matches = TOKEN_RGX.finditer(text)
    tokens = []
    contexts = []
    for match in matches:
        s = match.start()
        e = match.end()
        token = text[s:e]
        tokens.append(token)
        ws = max([0, s-window])
        we = min([e+window, len(text)])
        context = text[ws:we]
        contexts.append(context)
    return tokens, contexts


def read(path):
    with open(path, 'r', encoding='utf-8') as fin:
        return fin.read()

def readlines(path):
    with open(path, 'r', encoding='utf-8') as fin:
        return [t.strip() for t in fin.readlines()]

def strip_accents(s):
   return ''.join(c for c in unicodedata.normalize('NFD', s)
                  if unicodedata.category(c) != 'Mn')


def files_in_folder(path):
    lista_fisiere = []
    for fisier in os.listdir(path):
        cale_absoluta = os.path.join(path, fisier)
        if os.path.isfile(cale_absoluta):
            lista_fisiere.append(cale_absoluta)
    # sortam lista ca sa ne asiguram ca ordinea 
    # in care apar nu este una arbitrara
    lista_fisiere.sort()
    return lista_fisiere

def folders_in_folder(path):
    lista_fisiere = []
    for fisier in os.listdir(path):
        cale_absoluta = os.path.join(path, fisier)
        if os.path.isdir(cale_absoluta):
            lista_fisiere.append(cale_absoluta)
    # sortam lista ca sa ne asiguram ca ordinea 
    # in care apar nu este una arbitrara
    lista_fisiere.sort()
    return lista_fisiere

def grup_name_from_file(fis):
    return os.path.basename(fis).replace('.txt', '')

def read_wds(path_to_wds):
    wd_files = files_in_folder(path_to_wds)
    wds = {}
    for wd_fis in wd_files:
        wds[grup_name_from_file(wd_fis)] = set([strip_accents(wd) for wd in readlines(wd_fis)])
    return wds


def read_groupped_wds(path_to_groupped_wds):
    group_folders = folders_in_folder(path_to_groupped_wds)
    wds = defaultdict(set)
    wd2lemma = {}
    for gr_path in group_folders:
        gr_name = grup_name_from_file(gr_path)
        wd_files = files_in_folder(gr_path)
        for wd_fis in wd_files:
            wds[gr_name].update(set([strip_accents(wd.strip()) for wd in readlines(wd_fis)]))
            lemma = grup_name_from_file(wd_fis)
            for wd in set([strip_accents(wd.strip()) for wd in readlines(wd_fis)]):
                wd2lemma[wd] = lemma
    return wds, wd2lemma

def read_corpus(path_to_files):
    files = files_in_folder(path_to_files)
    corpus = []
    for fis in files:
        if '.cache' in fis:
            continue
        text = read(fis)
        corpus.append((fis, text))
    return corpus



def cache_file(fileo, doc):
    with open(fileo + '.cache', 'wb') as f:
        pickle.dump(doc, f)

def load_cache_file(fileo):
    if os.path.exists(fileo + '.cache'):
        with open(fileo + '.cache', 'rb') as f:
            return pickle.load(f)
    return None

def get_row_from_window(window, fis, tok, lemma, grup, context, has_ad):
    row = {'Fisier': fis, 'Lemma': lemma,  'Token': tok.lower(), 'Mentiune': tok, 'Grup': grup, 'Context': context, 'Has ad': has_ad}
    row["Tokenized"] = ' '.join(window)
    return row

late_dir = './late'
early_dir = './early'
wds_to_match, wd2lemma = read_groupped_wds('./wds/grupuri')

infolders = [late_dir, early_dir]

for infolder in infolders:
    corpus = read_corpus(infolder)
    window_size = 10

    rows = []
    for fis, text in corpus:
        print(fis)
        tokens, contexts = tokenize_regex(text, 80)
        for idx, (tok, context) in enumerate(zip(tokens, contexts)):
            start = max([0, idx - window_size])
            end = min([idx + window_size, len(tokens)])
            sm_start = max([0, idx - 3])
            sm_end = min([idx + 3, len(tokens)])
            for grup, grup_wds in wds_to_match.items():
                if tok.lower() in grup_wds:
                    #window = tokens[start:end]
                    left_window = tokens[start:idx]
                    right_window = tokens[idx:end]
                    if 'ad' in left_window:
                        lemma = wd2lemma[tok.lower()]
                        if 'ad' in right_window:
                            row = get_row_from_window(tokens[start:end], fis, tok, lemma, grup, context, True)
                        else:
                            row = get_row_from_window(tokens[start:sm_end], fis, tok, lemma, grup, context, True)
                        rows.append(row)
                    elif 'ad' in right_window:
                        lemma = wd2lemma[tok.lower()]
                        row = get_row_from_window(tokens[sm_start:end], fis, tok, lemma, grup, context, True)
                        rows.append(row)                
                    else:
                        lemma = wd2lemma[tok.lower()]
                        row = get_row_from_window(tokens[sm_start:sm_end], fis, tok, lemma, grup, context, False)
                        rows.append(row)


df = pd.DataFrame(rows)
df.to_excel(infolder+'.xlsx')

print('Done!')
