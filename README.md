# EthioMart Amharic Named Entity Recognition (NER) System

## Project Overview

EthioMart aims to centralize e-commerce activities from various Ethiopian-based Telegram channels, enabling real-time data extraction to create a unified platform. This project focuses on:

* Extracting real-time data (text, images, documents) from Telegram channels such as  **@ZemenExpress** .
* Fine-tuning a large language model (LLM) for Amharic Named Entity Recognition (NER).
* Identifying key entities such as products, prices, and locations in Amharic text.

This repository contains code for data ingestion, preprocessing, and labeling for NER tasks, supporting EthioMart’s vision of a seamless e-commerce platform.

## Features

* **Real-Time Data Ingestion** : A custom scraper that connects to Telegram channels and fetches messages, images, and metadata.
* **Text Preprocessing** : Tokenization, normalization, and handling Amharic-specific linguistic features.
* **NER Labeling** : Annotating text data for NER tasks in the CoNLL format, labeling products, prices, and locations.

## Requirements

Make sure to install the required libraries:

pip install -r requirements.txt


## Installation

1. Clone the repository:

   git clone https://github.com/GuleWako/Realtime-data-extraction-from-telegram-channel.git

   cd Realtime-data-extraction-from-telegram-channel
2. Set up the virtual environment:

   python -m venv venv
   source venv/bin/activate
3. Install the dependencies:

   pip install -r requirements.txt
4. Set up your Telegram API keys in the `.env` file:

   TG_API_ID=your_api_id
   TG_API_HASH=your_api_hash


   ## Data


   * The dataset includes real-time messages extracted from Telegram channels.
   * Entities labeled in CoNLL format include:
     * **B-Product** ,  **I-Product** : Product names
     * **B-PRICE** ,  **I-PRICE** : Price amounts
     * **B-LOC** ,  **I-LOC** : Location names
